/* src/test/modules/bark_wildcard/bark_wildcard--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION bark_wildcard" to load this file. \quit

-- Procedure 5: the column's options, include and exclude.
CREATE FUNCTION bark_wildcard_options(internal)
RETURNS void
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE PARALLEL SAFE;

-- Procedure 7, with markers: a document's [path, value] keys.
CREATE FUNCTION bark_wildcard_extract_value(jsonb, internal, internal,
                                            internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- Procedure 7's keys and markers of a document, for tests.
CREATE FUNCTION bark_wildcard_keys(doc jsonb, include text DEFAULT NULL,
                                   exclude text DEFAULT NULL)
RETURNS SETOF jsonb
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE PARALLEL SAFE;

-- The operators: doc OP [path, value] compares the values the class indexes
-- at path with value, within value's type (see bark_wildcard.c); doc #? path
-- says the document has such a value at path.
CREATE FUNCTION bark_wildcard_lt(jsonb, jsonb) RETURNS bool
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE FUNCTION bark_wildcard_le(jsonb, jsonb) RETURNS bool
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE FUNCTION bark_wildcard_eq(jsonb, jsonb) RETURNS bool
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE FUNCTION bark_wildcard_ge(jsonb, jsonb) RETURNS bool
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE FUNCTION bark_wildcard_gt(jsonb, jsonb) RETURNS bool
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE FUNCTION bark_wildcard_exists(jsonb, text) RETURNS bool
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OPERATOR #< (LEFTARG = jsonb, RIGHTARG = jsonb,
                    FUNCTION = bark_wildcard_lt,
                    RESTRICT = matchingsel, JOIN = matchingjoinsel);
CREATE OPERATOR #<= (LEFTARG = jsonb, RIGHTARG = jsonb,
                     FUNCTION = bark_wildcard_le,
                     RESTRICT = matchingsel, JOIN = matchingjoinsel);
CREATE OPERATOR #= (LEFTARG = jsonb, RIGHTARG = jsonb,
                    FUNCTION = bark_wildcard_eq,
                    RESTRICT = matchingsel, JOIN = matchingjoinsel);
CREATE OPERATOR #>= (LEFTARG = jsonb, RIGHTARG = jsonb,
                     FUNCTION = bark_wildcard_ge,
                     RESTRICT = matchingsel, JOIN = matchingjoinsel);
CREATE OPERATOR #> (LEFTARG = jsonb, RIGHTARG = jsonb,
                    FUNCTION = bark_wildcard_gt,
                    RESTRICT = matchingsel, JOIN = matchingjoinsel);
CREATE OPERATOR #? (LEFTARG = jsonb, RIGHTARG = text,
                    FUNCTION = bark_wildcard_exists,
                    RESTRICT = matchingsel, JOIN = matchingjoinsel);

-- doc |<| path: the smallest key [path, value] at path, NULL if none, for
-- ORDER BY doc |<| 'path' (MongoDB's sort by a path).  It returns the key,
-- jsonb, the class's storage type, because the scan reports the key it
-- meets first as the ORDER BY value; jsonb's btree family sorts it.
CREATE FUNCTION bark_wildcard_least(jsonb, text) RETURNS jsonb
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OPERATOR |<| (LEFTARG = jsonb, RIGHTARG = text,
                     FUNCTION = bark_wildcard_least);

-- Procedure 8, with BarkQueryFlags: a query's boundaries inside its path.
-- Its first argument is the operator's right operand: jsonb [path, value],
-- or text for #? and |<|.
CREATE FUNCTION bark_wildcard_extract_query(jsonb, int2, internal, internal,
                                            internal, internal, internal)
RETURNS void
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- Procedure 9: maybe, for a query on a path the projection drops.
CREATE FUNCTION bark_wildcard_recheck(jsonb, int2, internal)
RETURNS int2
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- Procedure 8's boundaries of a query under a projection, for tests.
CREATE FUNCTION bark_wildcard_boundaries(query anyelement, strategy int2,
                                         include text DEFAULT NULL,
                                         exclude text DEFAULT NULL)
RETURNS text
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE PARALLEL SAFE;

-- Is key inside procedure 8's boundaries of a query (no projection)?
CREATE FUNCTION bark_wildcard_in_boundaries(key jsonb, query anyelement,
                                            strategy int2)
RETURNS bool
AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- The (key, heap TID) members of a BARK index's wildcard column, for tests.
CREATE FUNCTION bark_wildcard_entries(index regclass, OUT key jsonb,
                                      OUT tid tid, OUT marker bool)
RETURNS SETOF record
AS 'MODULE_PATHNAME' LANGUAGE C STRICT;

CREATE OPERATOR FAMILY bark_jsonb_wildcard_ops USING bark;

-- Keys and markers are jsonb, compared with jsonb_cmp; procedure 1 is
-- registered under the input type, as BARK looks it up.  No equalimage:
-- jsonb's 1.0 and 1.00 are equal with different images, so entries do not
-- coalesce.  The comparison operators have btree's strategy numbers; they
-- reach only procedures 8 and 9.
CREATE OPERATOR CLASS bark_jsonb_wildcard_ops
FOR TYPE jsonb USING bark FAMILY bark_jsonb_wildcard_ops AS
    OPERATOR 1 #< (jsonb, jsonb),
    OPERATOR 2 #<= (jsonb, jsonb),
    OPERATOR 3 #= (jsonb, jsonb),
    OPERATOR 4 #>= (jsonb, jsonb),
    OPERATOR 5 #> (jsonb, jsonb),
    OPERATOR 6 #? (jsonb, text),
    OPERATOR 7 |<| (jsonb, text) FOR ORDER BY jsonb_ops,
    FUNCTION 1 (jsonb, jsonb) jsonb_cmp(jsonb, jsonb),
    FUNCTION 5 (jsonb, jsonb) bark_wildcard_options(internal),
    FUNCTION 7 (jsonb, jsonb) bark_wildcard_extract_value(jsonb, internal,
                                                           internal, internal,
                                                           internal),
    FUNCTION 8 (jsonb, jsonb) bark_wildcard_extract_query(jsonb, int2, internal,
                                                           internal, internal,
                                                           internal, internal),
    FUNCTION 9 (jsonb, jsonb) bark_wildcard_recheck(jsonb, int2, internal),
    STORAGE jsonb;
