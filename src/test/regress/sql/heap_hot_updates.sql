-- Create a function to measure HOT updates
CREATE OR REPLACE FUNCTION check_hot_updates(
    expected INT,
    p_table_name TEXT DEFAULT 't',
    p_schema_name TEXT DEFAULT current_schema()
)
RETURNS TABLE (
    table_name TEXT,
    total_updates BIGINT,
    hot_updates BIGINT,
    hot_update_percentage NUMERIC,
    matches_expected BOOLEAN,
    has_indexes BOOLEAN,
    index_count INT,
    fillfactor INT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_relid oid;
    v_qualified_name TEXT;
    v_hot_updates BIGINT;
    v_updates BIGINT;
    v_xact_hot_updates BIGINT;
    v_xact_updates BIGINT;
BEGIN

    -- We need to wait for statistics to update
    PERFORM pg_stat_force_next_flush();

    -- Construct qualified name
    v_qualified_name := quote_ident(p_schema_name) || '.' || quote_ident(p_table_name);

    -- Get the OID using regclass
    v_relid := v_qualified_name::regclass;

    IF v_relid IS NULL THEN
        RAISE EXCEPTION 'Table %.% not found', p_schema_name, p_table_name;
    END IF;

    -- Get cumulative stats
    v_hot_updates := COALESCE(pg_stat_get_tuples_hot_updated(v_relid), 0);
    v_updates := COALESCE(pg_stat_get_tuples_updated(v_relid), 0);

    -- Get current transaction stats
    v_xact_hot_updates := COALESCE(pg_stat_get_xact_tuples_hot_updated(v_relid), 0);
    v_xact_updates := COALESCE(pg_stat_get_xact_tuples_updated(v_relid), 0);

    -- Combine stats
    v_hot_updates := v_hot_updates + v_xact_hot_updates;
    v_updates := v_updates + v_xact_updates;

    RETURN QUERY
    SELECT
        p_table_name::TEXT,
        v_updates::BIGINT as total_updates,
        v_hot_updates::BIGINT as hot_updates,
        CASE
            WHEN v_updates > 0 THEN
                ROUND((v_hot_updates::numeric / v_updates::numeric * 100)::numeric, 2)
            ELSE 0
        END as hot_update_percentage,
        (v_hot_updates = expected)::BOOLEAN as matches_expected,
        (EXISTS (
            SELECT 1 FROM pg_index WHERE indrelid = v_relid
        ))::BOOLEAN as has_indexes,
        (
            SELECT COUNT(*)::INT
            FROM pg_index
            WHERE indrelid = v_relid
        ) as index_count,
        COALESCE(
            (
                SELECT (regexp_match(array_to_string(reloptions, ','), 'fillfactor=(\d+)'))[1]::int
                FROM pg_class
                WHERE oid = v_relid
            ),
            100
        ) as fillfactor;
END;
$$;

-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
-- This table will have two columns and two indexes, one on the primary key
-- id and one on the expression (docs->>'name').  That means that the indexed
-- attributes are 'id' and 'docs'.
CREATE TABLE t(id INT PRIMARY KEY, docs JSONB) WITH (autovacuum_enabled = off, fillfactor = 70);
CREATE INDEX t_docs_idx ON t((docs->>'name'));
INSERT INTO t VALUES (1, '{"name": "john", "data": "some data"}');

-- Disable expression checks.
ALTER TABLE t SET (expression_checks = false);
SELECT reloptions FROM pg_class WHERE relname = 't';

-- While the indexed attribute "name" is unchanged we've disabled expression
-- checks so this update should not go HOT as the system can't determine if
-- the indexed attribute has changed without evaluating the expression.
update t set docs='{"name": "john", "data": "something else"}' where id=1;
SELECT * FROM check_hot_updates(0);

-- Re-enable expression checks.
ALTER TABLE t SET (expression_checks = true);
SELECT reloptions FROM pg_class WHERE relname = 't';

-- The indexed attribute "name" with value "john" is unchanged, expect a HOT update.
UPDATE t SET docs='{"name": "john", "data": "some other data"}' WHERE id=1;
SELECT * FROM check_hot_updates(1);

-- The following update changes the indexed attribute "name", this should not be a HOT update.
UPDATE t SET docs='{"name": "smith", "data": "some other data"}' WHERE id=1;
SELECT * FROM check_hot_updates(1);

-- Now, this update does not change the indexed attribute "name" from "smith", this should be HOT.
UPDATE t SET docs='{"name": "smith", "data": "some more data"}' WHERE id=1;
SELECT * FROM check_hot_updates(2);

DROP TABLE t;


-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
-- This table is the same as the previous one but it has a third index.  The
-- index 'colindex' isn't an expression index, it indexes the entire value
-- in the docs column.  There are still only two indexed attributes for this
-- relation, the same two as before.  The presence of an index on the entire
-- value of the docs column should prevent HOT updates for any updates to any
-- portion of JSONB content in that column.
CREATE TABLE t(id INT PRIMARY KEY, docs JSONB) WITH (autovacuum_enabled = off, fillfactor = 70);
CREATE INDEX t_docs_idx ON t((docs->>'name'));
CREATE INDEX t_docs_col_idx ON t(docs);
INSERT INTO t VALUES (1, '{"name": "john", "data": "some data"}');

-- This update doesn't change the value of the expression index, but it does
-- change the content of the docs column and so should not be HOT because the
-- indexed value changed as a result of the update.
UPDATE t SET docs='{"name": "john", "data": "some other data"}' WHERE id=1;
SELECT * FROM check_hot_updates(0);
DROP TABLE t;


-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
-- The table has one column docs and two indexes.  They are both expression
-- indexes referencing the same column attribute (docs) but one is a partial
-- index.
CREATE TABLE t (docs JSONB) WITH (autovacuum_enabled = off, fillfactor = 70);
INSERT INTO t (docs) VALUES ('{"a": 0, "b": 0}');
INSERT INTO t (docs) SELECT jsonb_build_object('b', n) FROM generate_series(100, 10000) as n;
CREATE INDEX t_idx_a ON t ((docs->>'a'));
CREATE INDEX t_idx_b ON t ((docs->>'b')) WHERE (docs->>'b')::numeric > 9;

-- We're using BTREE indexes and for this test we want to make sure that they remain
-- in sync with changes to our relation.  Force the choice of index scans below so
-- that we know we're checking the index's understanding of what values should be
-- in the index or not.
SET SESSION enable_seqscan = OFF;
SET SESSION enable_bitmapscan = OFF;

-- Leave 'a' unchanged but modify 'b' to a value outside of the index predicate.
-- This should be a HOT update because neither index is changed.
UPDATE t SET docs = jsonb_build_object('a', 0, 'b', 1) WHERE (docs->>'a')::numeric = 0;
SELECT * FROM check_hot_updates(1);
-- Let's check to make sure that the index does not contain a value for 'b'
EXPLAIN (COSTS OFF) SELECT * FROM t WHERE (docs->>'b')::numeric > 9 AND (docs->>'b')::numeric < 100;
SELECT * FROM t WHERE (docs->>'b')::numeric > 9 AND (docs->>'b')::numeric < 100;

-- Leave 'a' unchanged but modify 'b' to a value within the index predicate.
-- This represents a change for field 'b' from unindexed to indexed and so
-- this should not take the HOT path.
UPDATE t SET docs = jsonb_build_object('a', 0, 'b', 10) WHERE (docs->>'a')::numeric = 0;
SELECT * FROM check_hot_updates(1);
-- Let's check to make sure that the index contains the new value of 'b'
EXPLAIN (COSTS OFF) SELECT * FROM t WHERE (docs->>'b')::numeric > 9 AND (docs->>'b')::numeric < 100;
SELECT * FROM t WHERE (docs->>'b')::numeric > 9 AND (docs->>'b')::numeric < 100;

-- This update modifies the value of 'a', an indexed field, so it also cannot
-- be a HOT update.
UPDATE t SET docs = jsonb_build_object('a', 1, 'b', 10) WHERE (docs->>'b')::numeric = 10;
SELECT * FROM check_hot_updates(1);

-- This update changes both 'a' and 'b' to new values that require index updates,
-- this cannot use the HOT path.
UPDATE t SET docs = jsonb_build_object('a', 2, 'b', 12) WHERE (docs->>'b')::numeric = 10;
SELECT * FROM check_hot_updates(1);
-- Let's check to make sure that the index contains the new value of 'b'
EXPLAIN (COSTS OFF) SELECT * FROM t WHERE (docs->>'b')::numeric > 9 AND (docs->>'b')::numeric < 100;
SELECT * FROM t WHERE (docs->>'b')::numeric > 9 AND (docs->>'b')::numeric < 100;

-- This update changes 'b' to a value outside its predicate requiring that
-- we remove it from the index.  That's a transition that can't be done
-- during a HOT update.
UPDATE t SET docs = jsonb_build_object('a', 2, 'b', 1) WHERE (docs->>'b')::numeric = 12;
SELECT * FROM check_hot_updates(1);
-- Let's check to make sure that the index no longer contains the value of 'b'
EXPLAIN (COSTS OFF) SELECT * FROM t WHERE (docs->>'b')::numeric > 9 AND (docs->>'b')::numeric < 100;
SELECT * FROM t WHERE (docs->>'b')::numeric > 9 AND (docs->>'b')::numeric < 100;

DROP TABLE t;
SET SESSION enable_seqscan = ON;
SET SESSION enable_bitmapscan = ON;

-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
-- Tests to ensure that HOT updates are not performed when multiple indexed
-- attributes are updated.
CREATE TABLE t(a INT, b INT) WITH (autovacuum_enabled = off, fillfactor = 70);
CREATE INDEX t_idx_a ON t(a);
CREATE INDEX t_idx_b ON t(abs(b));
INSERT INTO t VALUES (1, -1);

-- Both are updated, the second is an expression index with an unchanged
-- index value.  The change to the index on a should prevent HOT updates.
UPDATE t SET a = 2, b = 1 WHERE a = 1;
SELECT * FROM check_hot_updates(0);

DROP TABLE t;


-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
-- Tests to check the expression_checks reloption behavior.
--
CREATE TABLE t(a INT, b INT) WITH (autovacuum_enabled = off, fillfactor = 70);
CREATE INDEX t_idx_a ON t(abs(a)) WHERE abs(a) > 10;
CREATE INDEX t_idx_b ON t(abs(b));
INSERT INTO t VALUES (-1, -1), (-2, -2), (-3, -3), (-4, -4);

SET SESSION enable_seqscan = OFF;
SET SESSION enable_bitmapscan = OFF;

-- Disable expression checks on indexes and partial index predicates.
ALTER TABLE t SET (expression_checks = false);

-- Before and after values of a are outside the predicate of the index and
-- the indexed value of b hasn't changed however we've disabled expression
-- checks so this should not be a HOT update.
-- (-1, -1) -> (-5, -1)
UPDATE t SET a = -5, b = -1 WHERE a = -1;
SELECT * FROM check_hot_updates(0);

-- Enable expression checks on indexes, but not on predicates yet.
ALTER TABLE t SET (expression_checks = true);

-- The indexed value of b hasn't changed, this should be a HOT update.
-- (-5, -1) -> (-5, 1)
UPDATE t SET b = 1 WHERE a = -5;
SELECT * FROM check_hot_updates(1);

-- Now that we're not checking the predicate of the partial index, this
-- update of a from -5 to 5 should be HOT because we should ignore the
-- predicate and check the expression and find it unchanged.
-- (-5, 1) -> (5, 1)
UPDATE t SET a = 5 WHERE a = -5;
SELECT * FROM check_hot_updates(2);

-- This update meets the critera for the partial index and should not
-- be HOT.  Let's make sure of that and check the index as well.
-- (-4, -4) -> (-11, -4)
UPDATE t SET a = -11 WHERE a = -4;
SELECT * FROM check_hot_updates(2);
EXPLAIN (COSTS OFF) SELECT * FROM t WHERE abs(a) > 10;
SELECT * FROM t WHERE abs(a) > 10;

-- (-11, -4) -> (11, -4)
UPDATE t SET a = 11 WHERE a = -11;
SELECT * FROM check_hot_updates(3);
EXPLAIN (COSTS OFF) SELECT * FROM t WHERE abs(a) > 10;
SELECT * FROM t WHERE abs(a) > 10;

-- (11, -4) -> (-4, -4)
UPDATE t SET a = -4 WHERE a = 11;
SELECT * FROM check_hot_updates(3);
EXPLAIN (COSTS OFF) SELECT * FROM t WHERE abs(a) > 10;
SELECT * FROM t WHERE abs(a) > 10;

-- This update of a from 5 to -1 is HOT despite that attribute
-- being indexed because the before and after values for the
-- partial index predicate are outside the index definition.
-- (5, 1) -> (-1, 1)
UPDATE t SET a = -1 WHERE a = 5;
SELECT * FROM check_hot_updates(4);

-- This update of a from -2 to -1 with predicate checks enabled should be
-- HOT because the before/after values of a are both outside the predicate
-- of the partial index.
-- (-1, 1) -> (-2, 1)
UPDATE t SET a = -2 WHERE a = -1;
SELECT * FROM check_hot_updates(5);

-- The indexed value for b isn't changing, this should be HOT.
-- (-2, -2) -> (-2, 2)
UPDATE t SET b = 2 WHERE b = -2;
SELECT * FROM check_hot_updates(6);
EXPLAIN (COSTS OFF) SELECT abs(b) FROM t;
SELECT abs(b) FROM t;

-- Before and after values for a are outside the predicate of the index,
-- and because we're checking this should be HOT.
-- (-2, 1) -> (5, 1)
-- (-2, -2) -> (5, -2)
UPDATE t SET a = 5 WHERE a = -2;
SELECT * FROM check_hot_updates(8);

EXPLAIN (COSTS OFF) SELECT * FROM t WHERE abs(a) > 10;
SELECT * FROM t WHERE abs(a) > 10;

SELECT * FROM t;

DROP TABLE t;
SET SESSION enable_seqscan = ON;
SET SESSION enable_bitmapscan = ON;


-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
-- The tests here examines the behavior of HOT updates when the relation
-- has a JSONB column with an index on the field 'a' and the partial index
-- expression on a different JSONB field 'b'.
CREATE TABLE t(docs JSONB) WITH (autovacuum_enabled = off, fillfactor = 70);
CREATE INDEX t_docs_idx ON t((docs->'a')) WHERE (docs->'b')::integer = 1;
INSERT INTO t VALUES ('{"a": 1, "b": 1}');

EXPLAIN (COSTS OFF) SELECT * FROM t;
SELECT * FROM t;

SET SESSION enable_seqscan = OFF;
SET SESSION enable_bitmapscan = OFF;

EXPLAIN (COSTS OFF) SELECT * FROM t WHERE (docs->'b')::integer = 1;
SELECT * FROM t WHERE (docs->'b')::integer = 1;

SELECT * FROM check_hot_updates(0);

UPDATE t SET docs='{"a": 1, "b": 0}';
SELECT * FROM check_hot_updates(0);

SELECT * FROM t WHERE (docs->'b')::integer = 1;

SET SESSION enable_seqscan = ON;
SET SESSION enable_bitmapscan = ON;

DROP TABLE t;


-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
-- Tests for multi-column indexes
--
CREATE TABLE t(id INT, docs JSONB) WITH (autovacuum_enabled = off, fillfactor = 70);
CREATE INDEX t_docs_idx ON t(id, (docs->'a'));
INSERT INTO t VALUES (1, '{"a": 1, "b": 1}');

SET SESSION enable_seqscan = OFF;
SET SESSION enable_bitmapscan = OFF;

EXPLAIN (COSTS OFF) SELECT * FROM t WHERE id > 0 AND (docs->'a')::integer > 0;
SELECT * FROM t WHERE id > 0 AND (docs->'a')::integer > 0;

SELECT * FROM check_hot_updates(0);

-- Changing the id attribute which is an indexed attribute should
-- prevent HOT updates.
UPDATE t SET id = 2;
SELECT * FROM check_hot_updates(0);

SELECT * FROM t WHERE id > 0 AND (docs->'a')::integer > 0;

-- Changing the docs->'a' field in the indexed attribute 'docs'
-- should prevent HOT updates.
UPDATE t SET docs='{"a": -2, "b": 1}';
SELECT * FROM check_hot_updates(0);

SELECT * FROM t WHERE id > 0 AND (docs->'a')::integer < 0;

-- Leaving the docs->'a' attribute unchanged means that the expression
-- is unchanged and because the 'id' attribute isn't in the modified
-- set the indexed tuple is unchanged, this can go HOT.
UPDATE t SET docs='{"a": -2, "b": 2}';
SELECT * FROM check_hot_updates(1);

SELECT * FROM t WHERE id > 0 AND (docs->'a')::integer < 0;

-- Here we change the 'id' attribute and the 'docs' attribute setting
-- the expression docs->'a' to a new value, this cannot be a HOT update.
UPDATE t SET id = 3, docs='{"a": 3, "b": 3}';
SELECT * FROM check_hot_updates(1);

SELECT * FROM t WHERE id > 0 AND (docs->'a')::integer > 0;

SET SESSION enable_seqscan = ON;
SET SESSION enable_bitmapscan = ON;

DROP TABLE t;

-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
-- This table has a single column 'email' and a unique constraint on it that
-- should preclude HOT updates.
CREATE TABLE users (
    user_id serial primary key,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    EXCLUDE USING btree (lower(email) WITH =)
);

-- Add some data to the table and then update it in ways that should and should
-- not be HOT updates.
INSERT INTO users (name, email) VALUES
('user1', 'user1@example.com'),
('user2', 'user2@example.com'),
('taken', 'taken@EXAMPLE.com'),
('you', 'you@domain.com'),
('taken', 'taken@domain.com');

-- Should fail because of the unique constraint on the email column.
UPDATE users SET email = 'user1@example.com' WHERE email = 'user2@example.com';
SELECT * FROM check_hot_updates(0, 'users');

-- Should succeed because the email column is not being updated and should go HOT.
UPDATE users SET name = 'foo' WHERE email = 'user1@example.com';
SELECT * FROM check_hot_updates(1, 'users');

-- Create a partial index on the email column, updates
CREATE INDEX idx_users_email_no_example ON users (lower(email)) WHERE lower(email) LIKE '%@example.com%';

-- An update that changes the email column but not the indexed portion of it and falls outside the constraint.
-- Shouldn't be a HOT update because of the exclusion constraint.
UPDATE users SET email = 'you+2@domain.com' WHERE name = 'you';
SELECT * FROM check_hot_updates(1, 'users');

-- An update that changes the email column but not the indexed portion of it and falls within the constraint.
-- Again, should fail constraint and fail to be a HOT update.
UPDATE users SET email = 'taken@domain.com' WHERE name = 'you';
SELECT * FROM check_hot_updates(1, 'users');

DROP TABLE users;

-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
-- Another test of constraints spoiling HOT updates, this time with a range.
CREATE TABLE events (
    id serial primary key,
    name VARCHAR(255) NOT NULL,
    event_time tstzrange,
    constraint no_screening_time_overlap exclude using gist (
        event_time WITH &&
    )
);

-- Add two non-overlapping events.
INSERT INTO events (id, event_time, name)
VALUES
    (1, '["2023-01-01 19:00:00", "2023-01-01 20:45:00"]', 'event1'),
    (2, '["2023-01-01 21:00:00", "2023-01-01 21:45:00"]', 'event2');

-- Update the first event to overlap with the second, should fail the constraint and not be HOT.
UPDATE events SET event_time = '["2023-01-01 20:00:00", "2023-01-01 21:45:00"]' WHERE id = 1;
SELECT * FROM check_hot_updates(0, 'events');

-- Update the first event to not overlap with the second, again not HOT due to the constraint.
UPDATE events SET event_time = '["2023-01-01 22:00:00", "2023-01-01 22:45:00"]' WHERE id = 1;
SELECT * FROM check_hot_updates(0, 'events');

-- Update the first event to not overlap with the second, this time we're HOT because we don't overlap with the constraint.
UPDATE events SET name = 'new name here' WHERE id = 1;
SELECT * FROM check_hot_updates(1, 'events');

DROP TABLE events;

-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
-- A test to ensure that only modified summarizing indexes are updated, not
-- all of them.
CREATE TABLE ex (id SERIAL primary key, att1 JSONB, att2 text, att3 text, att4 text) WITH (fillfactor = 60);
CREATE INDEX ex_expr1_idx ON ex USING btree((att1->'data'));
CREATE INDEX ex_sumr1_idx ON ex USING BRIN(att2);
CREATE INDEX ex_expr2_idx ON ex USING btree((att1->'a'));
CREATE INDEX ex_expr3_idx ON ex USING btree((att1->'b'));
CREATE INDEX ex_expr4_idx ON ex USING btree((att1->'c'));
CREATE INDEX ex_sumr2_idx ON ex USING BRIN(att3);
CREATE INDEX ex_sumr3_idx ON ex USING BRIN(att4);
CREATE INDEX ex_expr5_idx ON ex USING btree((att1->'d'));
INSERT INTO ex (att1, att2) VALUES ('{"data": []}'::json, 'nothing special');

SELECT * FROM ex;

-- Update att2 and att4 both are BRIN/summarizing indexes, this should be a HOT update and
-- only update two of the three summarizing indexes.
UPDATE ex SET att2 = 'special indeed', att4 = 'whatever';
SELECT * FROM check_hot_updates(1, 'ex');
SELECT * FROM ex;

-- Update att1 and att2, only one is BRIN/summarizing, this should NOT be a HOT update.
UPDATE ex SET att1 = att1 || '{"data": "howdy"}', att2 = 'special, so special';
SELECT * FROM check_hot_updates(1, 'ex');
SELECT * FROM ex;

-- Update att2, att3, and att4 all are BRIN/summarizing indexes, this should be a HOT update
-- and yet still update all three summarizing indexes.
UPDATE ex SET att2 = 'a', att3 = 'b', att4 = 'c';
SELECT * FROM check_hot_updates(2, 'ex');
SELECT * FROM ex;

-- Update att1, att2, and att3 all modified values are BRIN/summarizing indexes, this should be a HOT update
-- and yet still update all three summarizing indexes.
UPDATE ex SET att1 = '{"data": "howdy"}', att2 = 'd', att3 = 'e';
SELECT * FROM check_hot_updates(3, 'ex');
SELECT * FROM ex;

DROP TABLE ex;

-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
-- A test to ensure that summarizing indexes are not updated when they don't
-- change, but are updated when they do while not prefent HOT updates.
CREATE TABLE ex (att1 JSONB, att2 text) WITH (fillfactor = 60);
CREATE INDEX ex_expr1_idx ON ex USING btree((att1->'data'));
CREATE INDEX ex_sumr1_idx ON ex USING BRIN(att2);
INSERT INTO ex VALUES ('{"data": []}', 'nothing special');

-- Update the unindexed value of att1, this should be a HOT update and and should
-- update the summarizing index.
UPDATE ex SET att1 = att1 || '{"status": "stalemate"}';
SELECT * FROM check_hot_updates(1, 'ex');

-- Update the indexed value of att2, a summarized value, this is a summarized
-- only update and should use the HOT path while still triggering an update to
-- the summarizing BRIN index.
UPDATE ex SET att2 = 'special indeed';
SELECT * FROM check_hot_updates(2, 'ex');

-- Update to att1 doesn't change the indexed value while the update to att2 does,
-- this again is a summarized only update and should use the HOT path as well as
-- trigger an update to the BRIN index.
UPDATE ex SET att1 = att1 || '{"status": "checkmate"}', att2 = 'special, so special';
SELECT * FROM check_hot_updates(3, 'ex');

-- This updates both indexes, the expression index on att1 and the summarizing
-- index on att2.  This should not be a HOT update because there are modified
-- indexes and only some are summarized, not all.  This should force all
-- indexes to be updated.
UPDATE ex SET att1 = att1 || '{"data": [1,2,3]}', att2 = 'do you want to play a game?';
SELECT * FROM check_hot_updates(4, 'ex');

DROP TABLE ex;

-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
-- This test is for a table with a custom type and a custom operators on
-- the BTREE index.  The question is, when comparing values for equality
-- to determine if there are changes on the index or not... shouldn't we
-- be using the custom operators?

-- Create a type
CREATE TYPE my_custom_type AS (val int);

-- Comparison functions (returns boolean)
CREATE FUNCTION my_custom_lt(a my_custom_type, b my_custom_type) RETURNS boolean AS $$
BEGIN
    RETURN a.val < b.val;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE FUNCTION my_custom_le(a my_custom_type, b my_custom_type) RETURNS boolean AS $$
BEGIN
    RETURN a.val <= b.val;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE FUNCTION my_custom_eq(a my_custom_type, b my_custom_type) RETURNS boolean AS $$
BEGIN
    RETURN a.val = b.val;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE FUNCTION my_custom_ge(a my_custom_type, b my_custom_type) RETURNS boolean AS $$
BEGIN
    RETURN a.val >= b.val;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE FUNCTION my_custom_gt(a my_custom_type, b my_custom_type) RETURNS boolean AS $$
BEGIN
    RETURN a.val > b.val;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE FUNCTION my_custom_ne(a my_custom_type, b my_custom_type) RETURNS boolean AS $$
BEGIN
    RETURN a.val != b.val;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

-- Comparison function (returns -1, 0, 1)
CREATE FUNCTION my_custom_cmp(a my_custom_type, b my_custom_type) RETURNS int AS $$
BEGIN
    IF a.val < b.val THEN
        RETURN -1;
    ELSIF a.val > b.val THEN
        RETURN 1;
    ELSE
        RETURN 0;
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

-- Create the operators
CREATE OPERATOR < (
    LEFTARG = my_custom_type,
    RIGHTARG = my_custom_type,
    PROCEDURE = my_custom_lt,
    COMMUTATOR = >,
    NEGATOR = >=
);

CREATE OPERATOR <= (
    LEFTARG = my_custom_type,
    RIGHTARG = my_custom_type,
    PROCEDURE = my_custom_le,
    COMMUTATOR = >=,
    NEGATOR = >
);

CREATE OPERATOR = (
    LEFTARG = my_custom_type,
    RIGHTARG = my_custom_type,
    PROCEDURE = my_custom_eq,
    COMMUTATOR = =,
    NEGATOR = <>
);

CREATE OPERATOR >= (
    LEFTARG = my_custom_type,
    RIGHTARG = my_custom_type,
    PROCEDURE = my_custom_ge,
    COMMUTATOR = <=,
    NEGATOR = <
);

CREATE OPERATOR > (
    LEFTARG = my_custom_type,
    RIGHTARG = my_custom_type,
    PROCEDURE = my_custom_gt,
    COMMUTATOR = <,
    NEGATOR = <=
);

CREATE OPERATOR <> (
    LEFTARG = my_custom_type,
    RIGHTARG = my_custom_type,
    PROCEDURE = my_custom_ne,
    COMMUTATOR = <>,
    NEGATOR = =
);

-- Create the operator class (including the support function)
CREATE OPERATOR CLASS my_custom_ops
    DEFAULT FOR TYPE my_custom_type USING btree AS
    OPERATOR 1 <,
    OPERATOR 2 <=,
    OPERATOR 3 =,
    OPERATOR 4 >=,
    OPERATOR 5 >,
    FUNCTION 1 my_custom_cmp(my_custom_type, my_custom_type);

-- Create the table
CREATE TABLE my_table (
    id int,
    custom_val my_custom_type
);

-- Insert some data
INSERT INTO my_table (id, custom_val) VALUES
(1, ROW(3)::my_custom_type),
(2, ROW(1)::my_custom_type),
(3, ROW(4)::my_custom_type),
(4, ROW(2)::my_custom_type);

-- Create a function to use when indexing
CREATE OR REPLACE FUNCTION abs_val(val my_custom_type) RETURNS int AS $$
BEGIN
  RETURN abs(val.val);
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

-- Create the index
CREATE INDEX idx_custom_val_abs ON my_table (abs_val(custom_val));

-- Update 1
UPDATE my_table SET custom_val = ROW(5)::my_custom_type WHERE id = 1;
SELECT * FROM check_hot_updates(0, 'my_table');

-- Update 2
UPDATE my_table SET custom_val = ROW(0)::my_custom_type WHERE custom_val < ROW(3)::my_custom_type;
SELECT * FROM check_hot_updates(0, 'my_table');

-- Update 3
UPDATE my_table SET custom_val = ROW(6)::my_custom_type WHERE id = 3;
SELECT * FROM check_hot_updates(0, 'my_table');

-- Update 4
UPDATE my_table SET id = 5 WHERE id = 1;
SELECT pg_stat_get_xact_tuples_hot_updated('my_table'::regclass);
SELECT * FROM check_hot_updates(0, 'my_table');

-- Query using the index
EXPLAIN (COSTS OFF) SELECT * FROM my_table WHERE abs_val(custom_val) = 6;
SELECT * FROM my_table WHERE abs_val(custom_val) = 6;

-- Clean up
DROP TABLE my_table CASCADE;
DROP OPERATOR CLASS my_custom_ops USING btree CASCADE;
DROP OPERATOR < (my_custom_type, my_custom_type);
DROP OPERATOR <= (my_custom_type, my_custom_type);
DROP OPERATOR = (my_custom_type, my_custom_type);
DROP OPERATOR >= (my_custom_type, my_custom_type);
DROP OPERATOR > (my_custom_type, my_custom_type);
DROP OPERATOR <> (my_custom_type, my_custom_type);
DROP FUNCTION my_custom_lt(my_custom_type, my_custom_type);
DROP FUNCTION my_custom_le(my_custom_type, my_custom_type);
DROP FUNCTION my_custom_eq(my_custom_type, my_custom_type);
DROP FUNCTION my_custom_ge(my_custom_type, my_custom_type);
DROP FUNCTION my_custom_gt(my_custom_type, my_custom_type);
DROP FUNCTION my_custom_ne(my_custom_type, my_custom_type);
DROP FUNCTION my_custom_cmp(my_custom_type, my_custom_type);
DROP FUNCTION abs_val(my_custom_type);
DROP TYPE my_custom_type CASCADE;

DROP FUNCTION check_hot_updates(int, text, text);
