/* src/test/modules/bark_multikey/bark_multikey--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION bark_multikey" to load this file. \quit

-- Procedure 7, extract-value: an array's elements, NULL elements flagged.
CREATE FUNCTION bark_multikey_extract_value(int4[], internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- Procedure 8, extract-query, with the optional seventh argument.
CREATE FUNCTION bark_multikey_extract_query(int4[], int2, internal, internal,
                                            internal, internal, internal)
RETURNS void
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- Procedure 9, index-recheck.
CREATE FUNCTION bark_multikey_recheck(int4, int2, internal)
RETURNS int2
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- The element-order operator: an array's least element.  The right operand
-- makes it binary, as an index operator must be; its value is not used.
CREATE FUNCTION bark_multikey_least(int4[], int4)
RETURNS int4
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OPERATOR |<| (
    LEFTARG = int4[],
    RIGHTARG = int4,
    FUNCTION = bark_multikey_least
);

-- The class.  Its operators are core's array operators, numbered as GIN's
-- array_ops numbers them, and the element-order operator.  Procedures 1, 2,
-- 4 and 6 are core's int4 ones, over the storage type.  BARK looks every
-- support function up under the input type; procedure 1 has to say so,
-- since CREATE OPERATOR CLASS would register a comparator under its
-- argument types.
CREATE OPERATOR FAMILY bark_int4_array_ops USING bark;

CREATE OPERATOR CLASS bark_int4_array_ops
FOR TYPE int4[] USING bark FAMILY bark_int4_array_ops AS
    OPERATOR 1 && (anyarray, anyarray),
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
    STORAGE int4;

-- Operators over an array's elements, for the range class below: some
-- element is greater than x, at most x, in [q[1], q[2]), an even element
-- of q, or equal to x; no element above x; some element other than x; some
-- element near x / 10 (see bark_multikey_extract_query).  |!| is always
-- true; its class's procedures misbehave on purpose.
CREATE FUNCTION bark_multikey_any_gt(int4[], int4) RETURNS bool
  AS 'SELECT EXISTS (SELECT 1 FROM unnest($1) e WHERE e > $2)'
  LANGUAGE sql IMMUTABLE STRICT;
CREATE FUNCTION bark_multikey_any_le(int4[], int4) RETURNS bool
  AS 'SELECT EXISTS (SELECT 1 FROM unnest($1) e WHERE e <= $2)'
  LANGUAGE sql IMMUTABLE STRICT;
CREATE FUNCTION bark_multikey_any_within(int4[], int4[]) RETURNS bool
  AS 'SELECT EXISTS (SELECT 1 FROM unnest($1) e WHERE e >= $2[1] AND e < $2[2])'
  LANGUAGE sql IMMUTABLE STRICT;
CREATE FUNCTION bark_multikey_any_even_in(int4[], int4[]) RETURNS bool
  AS 'SELECT EXISTS (SELECT 1 FROM unnest($1) e WHERE e % 2 = 0 AND e = ANY ($2))'
  LANGUAGE sql IMMUTABLE STRICT;
CREATE FUNCTION bark_multikey_has(int4[], int4) RETURNS bool
  AS 'SELECT $2 = ANY ($1)' LANGUAGE sql IMMUTABLE STRICT;
CREATE FUNCTION bark_multikey_none_gt(int4[], int4) RETURNS bool
  AS 'SELECT NOT EXISTS (SELECT 1 FROM unnest($1) e WHERE e > $2)'
  LANGUAGE sql IMMUTABLE STRICT;
CREATE FUNCTION bark_multikey_any_ne(int4[], int4) RETURNS bool
  AS 'SELECT EXISTS (SELECT 1 FROM unnest($1) e WHERE e <> $2)'
  LANGUAGE sql IMMUTABLE STRICT;
CREATE FUNCTION bark_multikey_any_near(int4[], int4) RETURNS bool
  AS 'SELECT EXISTS (SELECT 1 FROM unnest($1) e
                     WHERE (e > $2 / 10 - 2 OR (e = $2 / 10 - 2 AND $2 % 10 & 1 = 0))
                       AND ($2 % 10 & 4 = 4 OR e < $2 / 10 + 2 OR
                            (e = $2 / 10 + 2 AND $2 % 10 & 2 = 0)))'
  LANGUAGE sql IMMUTABLE STRICT;
CREATE FUNCTION bark_multikey_true(int4[], int4) RETURNS bool
  AS 'SELECT true' LANGUAGE sql IMMUTABLE STRICT;
CREATE OPERATOR |>| (LEFTARG = int4[], RIGHTARG = int4,
                     FUNCTION = bark_multikey_any_gt);
CREATE OPERATOR |<=| (LEFTARG = int4[], RIGHTARG = int4,
                      FUNCTION = bark_multikey_any_le);
CREATE OPERATOR |><| (LEFTARG = int4[], RIGHTARG = int4[],
                      FUNCTION = bark_multikey_any_within);
CREATE OPERATOR |%| (LEFTARG = int4[], RIGHTARG = int4[],
                     FUNCTION = bark_multikey_any_even_in);
CREATE OPERATOR |=| (LEFTARG = int4[], RIGHTARG = int4,
                     FUNCTION = bark_multikey_has);
CREATE OPERATOR |?| (LEFTARG = int4[], RIGHTARG = int4,
                     FUNCTION = bark_multikey_none_gt);
CREATE OPERATOR |<>| (LEFTARG = int4[], RIGHTARG = int4,
                      FUNCTION = bark_multikey_any_ne);
CREATE OPERATOR |~| (LEFTARG = int4[], RIGHTARG = int4,
                     FUNCTION = bark_multikey_any_near);
CREATE OPERATOR |!| (LEFTARG = int4[], RIGHTARG = int4,
                     FUNCTION = bark_multikey_true);

-- The range class: the same keys as bark_int4_array_ops, with operators
-- whose boundaries are ranges, strict ends and procedure 9 decisions, so
-- the scan's walk over boundaries is tested on more than points.  Like
-- GIN's extractQuery, procedure 8 reads its first argument as the type the
-- strategy's operator takes, an int4 for all but |><| and |%|.
CREATE OPERATOR FAMILY bark_int4_array_range_ops USING bark;

CREATE OPERATOR CLASS bark_int4_array_range_ops
FOR TYPE int4[] USING bark FAMILY bark_int4_array_range_ops AS
    OPERATOR 1 && (anyarray, anyarray),
    OPERATOR 2 @> (anyarray, anyarray),
    OPERATOR 6 |>| (int4[], int4),
    OPERATOR 7 |<=| (int4[], int4),
    OPERATOR 8 |><| (int4[], int4[]),
    OPERATOR 9 |%| (int4[], int4[]),
    OPERATOR 10 |=| (int4[], int4),
    OPERATOR 11 |!| (int4[], int4),
    OPERATOR 12 |?| (int4[], int4),
    OPERATOR 13 |<>| (int4[], int4),
    OPERATOR 14 |~| (int4[], int4),
    FUNCTION 1 (int4[], int4[]) btint4cmp(int4, int4),
    FUNCTION 2 btint4sortsupport(internal),
    FUNCTION 4 btequalimage(oid),
    FUNCTION 7 bark_multikey_extract_value(int4[], internal, internal),
    FUNCTION 8 bark_multikey_extract_query(int4[], int2, internal, internal,
                                           internal, internal, internal),
    FUNCTION 9 bark_multikey_recheck(int4, int2, internal),
    STORAGE int4;

-- Run procedure 7 or 8 of a class as BARK would, and print the result.
CREATE FUNCTION bark_multikey_keys(opclass oid, value anyelement)
RETURNS text
AS 'MODULE_PATHNAME' LANGUAGE C STRICT;

CREATE FUNCTION bark_multikey_boundaries(opclass oid, query anyelement,
                                         strategy int2)
RETURNS text
AS 'MODULE_PATHNAME' LANGUAGE C STRICT;

-- Procedure 7 of a class with markers: the elements, and a marker (the key
-- -2147483648, which no element may be) for an array of two or more.
CREATE FUNCTION bark_multikey_extract_marked(int4[], internal, internal,
                                             internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OPERATOR FAMILY bark_int4_array_marked_ops USING bark;

CREATE OPERATOR CLASS bark_int4_array_marked_ops
FOR TYPE int4[] USING bark FAMILY bark_int4_array_marked_ops AS
    OPERATOR 1 && (anyarray, anyarray),
    FUNCTION 1 (int4[], int4[]) btint4cmp(int4, int4),
    FUNCTION 2 btint4sortsupport(internal),
    FUNCTION 4 btequalimage(oid),
    FUNCTION 7 bark_multikey_extract_marked(int4[], internal, internal,
                                            internal, internal),
    STORAGE int4;

-- What BARK stored: the meta page's multikey flag and member count; every
-- (key, heap TID) member of an index whose first column is int4, in index
-- order; and whether a marker key is in an index.
CREATE FUNCTION bark_multikey_meta(index text, OUT multikey bool,
                                   OUT nkeys int8)
AS 'MODULE_PATHNAME' LANGUAGE C STRICT;

CREATE FUNCTION bark_multikey_entries(index text, OUT key int4, OUT tid tid,
                                      OUT marker bool)
RETURNS SETOF record
AS 'MODULE_PATHNAME' LANGUAGE C STRICT;

CREATE FUNCTION bark_multikey_has_marker(index text, key int4)
RETURNS bool
AS 'MODULE_PATHNAME' LANGUAGE C STRICT;

-- Scan an index on column 1 op query, mark after mark_at rows, read
-- read_after more, restore, and compare what follows with a plain scan.
CREATE FUNCTION bark_multikey_mark_restore(index text, op regoperator,
                                           query anyelement, mark_at int,
                                           read_after int)
RETURNS text
AS 'MODULE_PATHNAME' LANGUAGE C STRICT;
