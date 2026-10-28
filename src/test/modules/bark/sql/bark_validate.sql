--
-- barkvalidate: amvalidate() on BARK operator classes that each break one
-- rule, and barkadjustmembers' checks and dependencies.
--
-- A scalar BARK class lives in a btree operator family and keeps btree's
-- rules; a multikey class lives in an operator family of BARK's own.  Some
-- rules can be broken only by editing the catalogs, since CREATE OPERATOR
-- CLASS and ALTER OPERATOR FAMILY refuse the member: those cases update
-- pg_amop or pg_amproc and put the row back.
--
SET allow_system_table_mods = on;

-- Support functions with wrong signatures.  amvalidate reads only their
-- signatures; none is called.  The core refuses a wrong argument count,
-- argument type or result type for procedures 2, 5 and 6, but not a set
-- result.
CREATE FUNCTION bv_setof_void(internal) RETURNS SETOF void
  AS 'btint4sortsupport' LANGUAGE internal STRICT;
CREATE FUNCTION bv_inrange(int4, int8, int4, bool, bool) RETURNS bool
  AS 'in_range_int4_int4' LANGUAGE internal STRICT;
CREATE FUNCTION bv_eqimage(int4) RETURNS bool
  AS 'btequalimage' LANGUAGE internal STRICT;
CREATE FUNCTION bv_arraycmp(int4[], int4[]) RETURNS int4
  AS 'btarraycmp' LANGUAGE internal STRICT;
CREATE FUNCTION bv_extract_bad(int4, internal) RETURNS internal
  AS 'ginarrayextract' LANGUAGE internal STRICT;
CREATE FUNCTION bv_query(int4[], int2, internal, internal, internal, internal)
  RETURNS void AS 'ginqueryarrayextract' LANGUAGE internal STRICT;
CREATE FUNCTION bv_query_bad(int4[], int4, internal, internal, internal, internal)
  RETURNS void AS 'ginqueryarrayextract' LANGUAGE internal STRICT;
CREATE FUNCTION bv_recheck(int4, int2, internal) RETURNS int2
  AS 'ginarrayextract' LANGUAGE internal STRICT;
CREATE FUNCTION bv_recheck_bad(int4[], int2, internal) RETURNS int2
  AS 'ginarrayextract' LANGUAGE internal STRICT;
CREATE FUNCTION bv_gquery_bad(int4[], internal, int2, internal, internal)
  RETURNS bool AS 'ginqueryarrayextract' LANGUAGE internal STRICT;
CREATE FUNCTION bv_consistent_bad(internal, int2, int4[], int4, internal, internal)
  RETURNS int4 AS 'ginarrayconsistent' LANGUAGE internal STRICT;
CREATE FUNCTION bv_cmppartial(int4, int4, int2, internal) RETURNS int4
  AS 'gin_cmp_prefix' LANGUAGE internal STRICT;
CREATE FUNCTION bv_cmppartial_bad(int4[], int4[], int2, internal) RETURNS int4
  AS 'gin_cmp_prefix' LANGUAGE internal STRICT;
CREATE FUNCTION bv_triconsistent_bad(internal, int2, int4[], int4, internal,
                                     internal, internal)
  RETURNS bool AS 'ginarraytriconsistent' LANGUAGE internal STRICT;
CREATE FUNCTION bv_fetch(int4) RETURNS int4[]
  AS 'array_append' LANGUAGE internal STRICT;
CREATE FUNCTION bv_fetch_bad(int4[]) RETURNS int4[]
  AS 'array_append' LANGUAGE internal STRICT;
CREATE FUNCTION bv_fam(text) RETURNS oid
  AS 'SELECT oid FROM pg_opfamily WHERE opfname = $1' LANGUAGE sql STABLE;

--
-- Scalar classes, in btree families.
--
CREATE OPERATOR FAMILY bv_bt USING btree;
CREATE OPERATOR CLASS bv_bt FOR TYPE int4 USING bark FAMILY bv_bt AS
    OPERATOR 1 <, OPERATOR 3 =,
    FUNCTION 1 btint4cmp(int4, int4);
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bv_bt';

-- A wrong signature for each support function.  BARK's ALTER OPERATOR
-- FAMILY finds only BARK's own families, so members of a btree family are
-- added with btree's.  Procedure 1 takes the class's type.
CREATE OPERATOR FAMILY bv_cmp1 USING btree;
CREATE OPERATOR CLASS bv_cmp1 FOR TYPE int4 USING bark FAMILY bv_cmp1 AS
    OPERATOR 1 <,
    FUNCTION 1 (int4, int4) btint48cmp(int4, int8);
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bv_cmp1';
-- sortsupport returns void, not a set
ALTER OPERATOR FAMILY bv_bt USING btree ADD
    FUNCTION 2 (int4, int4) bv_setof_void(internal);
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bv_bt';
ALTER OPERATOR FAMILY bv_bt USING btree DROP FUNCTION 2 (int4, int4);
-- in_range's second argument has the left type
ALTER OPERATOR FAMILY bv_bt USING btree ADD
    FUNCTION 3 (int4, int4) bv_inrange(int4, int8, int4, bool, bool);
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bv_bt';
ALTER OPERATOR FAMILY bv_bt USING btree DROP FUNCTION 3 (int4, int4);
-- equalimage takes an oid
ALTER OPERATOR FAMILY bv_bt USING btree ADD
    FUNCTION 4 (int4, int4) bv_eqimage(int4);
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bv_bt';
ALTER OPERATOR FAMILY bv_bt USING btree DROP FUNCTION 4 (int4, int4);
-- options returns void, not a set; a right one is accepted
ALTER OPERATOR FAMILY bv_bt USING btree ADD
    FUNCTION 5 (int4, int4) bv_setof_void(internal);
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bv_bt';
ALTER OPERATOR FAMILY bv_bt USING btree DROP FUNCTION 5 (int4, int4);
ALTER OPERATOR FAMILY bv_bt USING btree ADD
    FUNCTION 5 (int4, int4) gtsvector_options(internal);
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bv_bt';
ALTER OPERATOR FAMILY bv_bt USING btree DROP FUNCTION 5 (int4, int4);
-- skipsupport returns void, not a set
ALTER OPERATOR FAMILY bv_bt USING btree ADD
    FUNCTION 6 (int4, int4) bv_setof_void(internal);
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bv_bt';
ALTER OPERATOR FAMILY bv_bt USING btree DROP FUNCTION 6 (int4, int4);

-- A support number above btree's six.  barkadjustmembers and btree refuse
-- one, so renumber a sortsupport function.
ALTER OPERATOR FAMILY bv_bt USING btree ADD
    FUNCTION 2 (int4, int4) btint4sortsupport(internal);
UPDATE pg_amproc SET amprocnum = 7
  WHERE amprocfamily = bv_fam('bv_bt') AND amprocnum = 2;
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bv_bt';
UPDATE pg_amproc SET amprocnum = 2
  WHERE amprocfamily = bv_fam('bv_bt') AND amprocnum = 7;
ALTER OPERATOR FAMILY bv_bt USING btree DROP FUNCTION 2 (int4, int4);

-- Search strategies below 1 and above 5.
ALTER OPERATOR FAMILY bv_bt USING btree ADD OPERATOR 2 <= (int4, int4);
UPDATE pg_amop SET amopstrategy = 0
  WHERE amopfamily = bv_fam('bv_bt') AND amopstrategy = 2;
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bv_bt';
UPDATE pg_amop SET amopstrategy = 6
  WHERE amopfamily = bv_fam('bv_bt') AND amopstrategy = 0;
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bv_bt';
UPDATE pg_amop SET amopstrategy = 2
  WHERE amopfamily = bv_fam('bv_bt') AND amopstrategy = 6;
ALTER OPERATOR FAMILY bv_bt USING btree DROP OPERATOR 2 (int4, int4);

-- A search operator registered for types it does not take.
UPDATE pg_amop SET amoprighttype = 'int8'::regtype
  WHERE amopfamily = bv_fam('bv_bt') AND amopstrategy = 3;
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bv_bt';
UPDATE pg_amop SET amoprighttype = 'int4'::regtype
  WHERE amopfamily = bv_fam('bv_bt') AND amopstrategy = 3;

-- An ordering operator: BARK's distance operator has the KNN strategy 6,
-- which barkadjustmembers refuses (a btree family keeps 1..5), so
-- CREATE OPERATOR CLASS can only give it a wrong one.  Renumbered to 6 it
-- is valid; without a sort family it is not.
CREATE OPERATOR FAMILY bv_knn USING btree;
CREATE OPERATOR CLASS bv_knn FOR TYPE int4 USING bark FAMILY bv_knn AS
    OPERATOR 1 <,
    OPERATOR 5 <~> (int4, int4) FOR ORDER BY float_ops,
    FUNCTION 1 btint4cmp(int4, int4);
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bv_knn';
UPDATE pg_amop SET amopstrategy = 6
  WHERE amopfamily = bv_fam('bv_knn') AND amoppurpose = 'o';
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bv_knn';
UPDATE pg_amop SET amopsortfamily = 0
  WHERE amopfamily = bv_fam('bv_knn') AND amoppurpose = 'o';
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bv_knn';
UPDATE pg_amop SET amopsortfamily = (SELECT oid FROM pg_opfamily
                                     WHERE opfname = 'float_ops' AND opfmethod = 403)
  WHERE amopfamily = bv_fam('bv_knn') AND amoppurpose = 'o';
-- barkadjustmembers refuses strategy 6 in a btree family.
CREATE OPERATOR CLASS bv_knn_bad FOR TYPE int8 USING bark FAMILY bv_knn AS
    OPERATOR 6 <~> (int8, int8) FOR ORDER BY float_ops,
    FUNCTION 1 btint8cmp(int8, int8);

-- No comparator, or only a cross-type one: the class has no order.  A
-- class with no operators has nothing wrong.
CREATE OPERATOR FAMILY bv_nocmp USING btree;
CREATE OPERATOR CLASS bv_nocmp FOR TYPE int4 USING bark FAMILY bv_nocmp AS
    OPERATOR 1 <;
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bv_nocmp';
ALTER OPERATOR FAMILY bv_nocmp USING btree ADD
    FUNCTION 1 (int4, int8) btint48cmp(int4, int8);
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bv_nocmp';
CREATE OPERATOR FAMILY bv_noops USING btree;
CREATE OPERATOR CLASS bv_noops FOR TYPE int4 USING bark FAMILY bv_noops AS
    FUNCTION 1 btint4cmp(int4, int4);
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bv_noops';

-- Cross-type gaps: barkvalidate does not check that a family's cross-type
-- operators have their comparator, as btvalidate does; it checks only the
-- class's own one.
ALTER OPERATOR FAMILY bv_bt USING btree ADD OPERATOR 1 < (int4, int8);
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bv_bt';
CREATE TABLE bv_t (a int4);
INSERT INTO bv_t SELECT generate_series(1, 100);
CREATE INDEX bv_t_a ON bv_t USING bark (a bv_bt);
SET enable_seqscan = off;
SET enable_bitmapscan = off;
EXPLAIN (COSTS OFF) SELECT count(*) FROM bv_t WHERE a < 5::int8;
SELECT count(*) FROM bv_t WHERE a < 5::int8;
RESET enable_seqscan;
RESET enable_bitmapscan;
SELECT count(*) FROM bv_t WHERE a < 5::int8;
DROP TABLE bv_t;

-- barkadjustmembers leaves the core's dependencies alone: a class's
-- members depend on the class (internal), ALTER OPERATOR FAMILY's on the
-- family (auto), and dropping the class keeps the family's.
CREATE FUNCTION bv_members(fam text)
RETURNS TABLE (member text, owner text, deptype "char") AS $$
  SELECT pg_describe_object(d.classid, d.objid, 0),
         pg_describe_object(d.refclassid, d.refobjid, 0), d.deptype
  FROM pg_depend d
  WHERE d.classid IN ('pg_amop'::regclass, 'pg_amproc'::regclass)
    AND ((d.refclassid = 'pg_opfamily'::regclass AND d.refobjid = bv_fam(fam))
         OR (d.refclassid = 'pg_opclass'::regclass AND d.refobjid IN
             (SELECT oid FROM pg_opclass WHERE opcfamily = bv_fam(fam))))
$$ LANGUAGE sql STABLE;
SELECT * FROM bv_members('bv_bt') ORDER BY member COLLATE "C", owner COLLATE "C";
DROP OPERATOR CLASS bv_bt USING bark;
SELECT * FROM bv_members('bv_bt') ORDER BY member COLLATE "C", owner COLLATE "C";

--
-- Multikey classes, in families of BARK's own.
--
-- The class's own members cannot be dropped, so the procedures under
-- test are the family's.
CREATE OPERATOR CLASS bv_mk FOR TYPE int4[] USING bark AS
    OPERATOR 1 && (anyarray, anyarray),
    STORAGE int4;
ALTER OPERATOR FAMILY bv_mk USING bark ADD
    FUNCTION 1 (int4[], int4[]) btint4cmp(int4, int4),
    FUNCTION 7 (int4[], int4[]) ginarrayextract(anyarray, internal, internal);
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bv_mk';
-- Without a storage type the keys are the input type's.
CREATE OPERATOR CLASS bv_mk_nostorage FOR TYPE int4[] USING bark AS
    OPERATOR 1 && (anyarray, anyarray),
    FUNCTION 1 (int4[], int4[]) bv_arraycmp(int4[], int4[]),
    FUNCTION 7 ginarrayextract(anyarray, internal, internal);
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bv_mk_nostorage';

-- Procedure 1: missing, then over the input type rather than the storage
-- type.  (Registered under the storage type it is missing too, with a
-- detail: bark_multikey's test.)
ALTER OPERATOR FAMILY bv_mk USING bark DROP FUNCTION 1 (int4[], int4[]);
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bv_mk';
ALTER OPERATOR FAMILY bv_mk USING bark ADD
    FUNCTION 1 (int4[], int4[]) bv_arraycmp(int4[], int4[]);
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bv_mk';
ALTER OPERATOR FAMILY bv_mk USING bark DROP FUNCTION 1 (int4[], int4[]);
ALTER OPERATOR FAMILY bv_mk USING bark ADD
    FUNCTION 1 (int4[], int4[]) btint4cmp(int4, int4);
-- 2: sortsupport
ALTER OPERATOR FAMILY bv_mk USING bark ADD
    FUNCTION 2 (int4[], int4[]) bv_setof_void(internal);
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bv_mk';
ALTER OPERATOR FAMILY bv_mk USING bark DROP FUNCTION 2 (int4[], int4[]);
-- 3: in_range is refused whatever its signature
ALTER OPERATOR FAMILY bv_mk USING bark ADD
    FUNCTION 3 (int4[], int4[]) in_range(int4, int4, int4, bool, bool);
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bv_mk';
ALTER OPERATOR FAMILY bv_mk USING bark DROP FUNCTION 3 (int4[], int4[]);
-- 4: equalimage
ALTER OPERATOR FAMILY bv_mk USING bark ADD
    FUNCTION 4 (int4[], int4[]) bv_eqimage(int4);
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bv_mk';
ALTER OPERATOR FAMILY bv_mk USING bark DROP FUNCTION 4 (int4[], int4[]);
-- 5: options
ALTER OPERATOR FAMILY bv_mk USING bark ADD
    FUNCTION 5 (int4[], int4[]) bv_setof_void(internal);
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bv_mk';
ALTER OPERATOR FAMILY bv_mk USING bark DROP FUNCTION 5 (int4[], int4[]);
-- 6: skipsupport
ALTER OPERATOR FAMILY bv_mk USING bark ADD
    FUNCTION 6 (int4[], int4[]) bv_setof_void(internal);
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bv_mk';
ALTER OPERATOR FAMILY bv_mk USING bark DROP FUNCTION 6 (int4[], int4[]);
-- 7: missing, then taking the storage type rather than the input type
ALTER OPERATOR FAMILY bv_mk USING bark DROP FUNCTION 7 (int4[], int4[]);
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bv_mk';
ALTER OPERATOR FAMILY bv_mk USING bark ADD
    FUNCTION 7 (int4[], int4[]) bv_extract_bad(int4, internal);
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bv_mk';
ALTER OPERATOR FAMILY bv_mk USING bark DROP FUNCTION 7 (int4[], int4[]);
ALTER OPERATOR FAMILY bv_mk USING bark ADD
    FUNCTION 7 (int4[], int4[]) ginarrayextract(anyarray, internal, internal);
-- 9 alone is fine; 8 with a strategy of int4, not int2
ALTER OPERATOR FAMILY bv_mk USING bark ADD
    FUNCTION 9 (int4[], int4[]) bv_recheck(int4, int2, internal);
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bv_mk';
ALTER OPERATOR FAMILY bv_mk USING bark ADD
    FUNCTION 8 (int4[], int4[]) bv_query_bad(int4[], int4, internal, internal, internal, internal);
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bv_mk';
ALTER OPERATOR FAMILY bv_mk USING bark DROP FUNCTION 8 (int4[], int4[]);
-- 8 needs 9; 9 takes the storage type
ALTER OPERATOR FAMILY bv_mk USING bark ADD
    FUNCTION 8 (int4[], int4[]) bv_query(int4[], int2, internal, internal, internal, internal);
ALTER OPERATOR FAMILY bv_mk USING bark DROP FUNCTION 9 (int4[], int4[]);
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bv_mk';
ALTER OPERATOR FAMILY bv_mk USING bark ADD
    FUNCTION 9 (int4[], int4[]) bv_recheck_bad(int4[], int2, internal);
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bv_mk';
ALTER OPERATOR FAMILY bv_mk USING bark DROP FUNCTION 9 (int4[], int4[]);
ALTER OPERATOR FAMILY bv_mk USING bark ADD
    FUNCTION 9 (int4[], int4[]) bv_recheck(int4, int2, internal);
-- 10-13: GIN's extractQuery, consistent, comparePartial, triConsistent
ALTER OPERATOR FAMILY bv_mk USING bark ADD
    FUNCTION 10 (int4[], int4[]) bv_gquery_bad(int4[], internal, int2, internal, internal);
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bv_mk';
ALTER OPERATOR FAMILY bv_mk USING bark DROP FUNCTION 10 (int4[], int4[]);
ALTER OPERATOR FAMILY bv_mk USING bark ADD
    FUNCTION 11 (int4[], int4[]) bv_consistent_bad(internal, int2, int4[], int4, internal, internal);
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bv_mk';
ALTER OPERATOR FAMILY bv_mk USING bark DROP FUNCTION 11 (int4[], int4[]);
ALTER OPERATOR FAMILY bv_mk USING bark ADD
    FUNCTION 12 (int4[], int4[]) bv_cmppartial_bad(int4[], int4[], int2, internal);
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bv_mk';
ALTER OPERATOR FAMILY bv_mk USING bark DROP FUNCTION 12 (int4[], int4[]);
ALTER OPERATOR FAMILY bv_mk USING bark ADD
    FUNCTION 13 (int4[], int4[]) bv_triconsistent_bad(internal, int2, int4[], int4, internal, internal, internal);
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bv_mk';
ALTER OPERATOR FAMILY bv_mk USING bark DROP FUNCTION 13 (int4[], int4[]);
-- 14: fetch takes the storage type
ALTER OPERATOR FAMILY bv_mk USING bark ADD
    FUNCTION 14 (int4[], int4[]) bv_fetch_bad(int4[]);
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bv_mk';
ALTER OPERATOR FAMILY bv_mk USING bark DROP FUNCTION 14 (int4[], int4[]);
-- Every procedure, with right signatures.
ALTER OPERATOR FAMILY bv_mk USING bark ADD
    FUNCTION 2 (int4[], int4[]) btint4sortsupport(internal),
    FUNCTION 4 (int4[], int4[]) btequalimage(oid),
    FUNCTION 5 (int4[], int4[]) gtsvector_options(internal),
    FUNCTION 6 (int4[], int4[]) btint4skipsupport(internal),
    FUNCTION 10 (int4[], int4[]) ginqueryarrayextract(anyarray, internal, int2, internal, internal, internal, internal),
    FUNCTION 11 (int4[], int4[]) ginarrayconsistent(internal, int2, anyarray, int4, internal, internal, internal, internal),
    FUNCTION 12 (int4[], int4[]) bv_cmppartial(int4, int4, int2, internal),
    FUNCTION 13 (int4[], int4[]) ginarraytriconsistent(internal, int2, anyarray, int4, internal, internal, internal),
    FUNCTION 14 (int4[], int4[]) bv_fetch(int4);
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bv_mk';

-- Cross-type support functions: from the input type, then from the
-- storage type.  BARK never looks them up.
ALTER OPERATOR FAMILY bv_mk USING bark ADD
    FUNCTION 1 (int4[], int8) btint48cmp(int4, int8);
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bv_mk';
ALTER OPERATOR FAMILY bv_mk USING bark DROP FUNCTION 1 (int4[], int8);
ALTER OPERATOR FAMILY bv_mk USING bark ADD
    FUNCTION 1 (int4, int8) btint48cmp(int4, int8);
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bv_mk';
ALTER OPERATOR FAMILY bv_mk USING bark DROP FUNCTION 1 (int4, int8);

-- An ordering operator needs a sort family; a search operator must be
-- registered for its own types.
ALTER OPERATOR FAMILY bv_mk USING bark ADD
    OPERATOR 20 + (int4, int4) FOR ORDER BY integer_ops;
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bv_mk';
UPDATE pg_amop SET amopsortfamily = 0
  WHERE amopfamily = bv_fam('bv_mk') AND amoppurpose = 'o';
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bv_mk';
UPDATE pg_amop SET amopsortfamily = (SELECT oid FROM pg_opfamily
                                     WHERE opfname = 'integer_ops' AND opfmethod = 403)
  WHERE amopfamily = bv_fam('bv_mk') AND amoppurpose = 'o';
ALTER OPERATOR FAMILY bv_mk USING bark DROP OPERATOR 20 (int4, int4);
UPDATE pg_amop SET amoprighttype = 'int4'::regtype
  WHERE amopfamily = bv_fam('bv_mk') AND amopstrategy = 1;
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bv_mk';
UPDATE pg_amop SET amoprighttype = 'anyarray'::regtype
  WHERE amopfamily = bv_fam('bv_mk') AND amopstrategy = 1;
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bv_mk';
SELECT * FROM bv_members('bv_mk') ORDER BY member COLLATE "C", owner COLLATE "C";

RESET allow_system_table_mods;
DROP OPERATOR FAMILY bv_bt USING btree;
DROP OPERATOR FAMILY bv_cmp1 USING btree;
DROP OPERATOR FAMILY bv_knn USING btree;
DROP OPERATOR FAMILY bv_nocmp USING btree;
DROP OPERATOR FAMILY bv_noops USING btree;
DROP OPERATOR FAMILY bv_mk USING bark;
DROP OPERATOR FAMILY bv_mk_nostorage USING bark;
DROP FUNCTION bv_members(text), bv_fam(text), bv_setof_void(internal),
  bv_inrange(int4, int8, int4, bool, bool), bv_eqimage(int4),
  bv_arraycmp(int4[], int4[]), bv_extract_bad(int4, internal),
  bv_query(int4[], int2, internal, internal, internal, internal),
  bv_query_bad(int4[], int4, internal, internal, internal, internal),
  bv_recheck(int4, int2, internal), bv_recheck_bad(int4[], int2, internal),
  bv_gquery_bad(int4[], internal, int2, internal, internal),
  bv_consistent_bad(internal, int2, int4[], int4, internal, internal),
  bv_cmppartial(int4, int4, int2, internal),
  bv_cmppartial_bad(int4[], int4[], int2, internal),
  bv_triconsistent_bad(internal, int2, int4[], int4, internal, internal, internal),
  bv_fetch(int4), bv_fetch_bad(int4[]);
