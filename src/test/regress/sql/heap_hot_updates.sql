-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
-- This table will have two columns and two indexes, one on the primary key
-- id and one on the expression (info->>'name').  That means that the indexed
-- attributes are 'id' and 'info'.
create table keyvalue(id integer primary key, info jsonb);
create index nameindex on keyvalue((info->>'name'));
insert into keyvalue values (1, '{"name": "john", "data": "some data"}');

-- Disable expression checks.
ALTER TABLE keyvalue SET (expression_checks = false);
SELECT reloptions FROM pg_class WHERE relname = 'keyvalue';

-- While the indexed attribute "name" is unchanged we've disabled expression
-- checks so this update should not go HOT as the system can't determine if
-- the indexed attribute has changed without evaluating the expression.
update keyvalue set info='{"name": "john", "data": "something else"}' where id=1;
select pg_stat_get_xact_tuples_hot_updated('keyvalue'::regclass); -- expect: 0 row

-- Re-enable expression checks.
ALTER TABLE keyvalue SET (expression_checks = true);
SELECT reloptions FROM pg_class WHERE relname = 'keyvalue';

-- The indexed attribute "name" with value "john" is unchanged, expect a HOT update.
update keyvalue set info='{"name": "john", "data": "some other data"}' where id=1;
select pg_stat_get_xact_tuples_hot_updated('keyvalue'::regclass); -- expect: 1 row

-- The following update changes the indexed attribute "name", this should not be a HOT update.
update keyvalue set info='{"name": "smith", "data": "some other data"}' where id=1;
select pg_stat_get_xact_tuples_hot_updated('keyvalue'::regclass); -- expect: 1 no new HOT updates

-- Now, this update does not change the indexed attribute "name" from "smith", this should be HOT.
update keyvalue set info='{"name": "smith", "data": "some more data"}' where id=1;
select pg_stat_get_xact_tuples_hot_updated('keyvalue'::regclass); -- expect: 2 rows now
drop table keyvalue;


-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
-- This table is the same as the previous one but it has a third index.  The
-- index 'colindex' isn't an expression index, it indexes the entire value
-- in the info column.  There are still only two indexed attributes for this
-- relation, the same two as before.  The presence of an index on the entire
-- value of the info column should prevent HOT updates for any updates to any
-- portion of JSONB content in that column.
create table keyvalue(id integer primary key, info jsonb);
create index nameindex on keyvalue((info->>'name'));
create index colindex on keyvalue(info);
insert into keyvalue values (1, '{"name": "john", "data": "some data"}');

-- This update doesn't change the value of the expression index, but it does
-- change the content of the info column and so should not be HOT because the
-- indexed value changed as a result of the update.
update keyvalue set info='{"name": "john", "data": "some other data"}' where id=1;
select pg_stat_get_xact_tuples_hot_updated('keyvalue'::regclass); -- expect: 0 rows
drop table keyvalue;

-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
-- The table has one column docs and two indexes.  They are both expression
-- indexes referencing the same column attribute (docs) but one is a partial
-- index.
CREATE TABLE ex (docs JSONB) WITH (fillfactor = 60);
INSERT INTO ex (docs) VALUES ('{"a": 0, "b": 0}');
INSERT INTO ex (docs) SELECT jsonb_build_object('b', n) FROM generate_series(100, 10000) as n;
CREATE INDEX idx_ex_a ON ex ((docs->>'a'));
CREATE INDEX idx_ex_b ON ex ((docs->>'b')) WHERE (docs->>'b')::numeric > 9;

-- We're using BTREE indexes and for this test we want to make sure that they remain
-- in sync with changes to our relation.  Force the choice of index scans below so
-- that we know we're checking the index's understanding of what values should be
-- in the index or not.
SET SESSION enable_seqscan = OFF;
SET SESSION enable_bitmapscan = OFF;

-- Leave 'a' unchanged but modify 'b' to a value outside of the index predicate.
-- This should be a HOT update because neither index is changed.
UPDATE ex SET docs = jsonb_build_object('a', 0, 'b', 1) WHERE (docs->>'a')::numeric = 0;
SELECT pg_stat_get_xact_tuples_hot_updated('ex'::regclass); -- expect: 1 row
-- Let's check to make sure that the index does not contain a value for 'b'
EXPLAIN (COSTS OFF) SELECT * FROM ex WHERE (docs->>'b')::numeric > 9 AND (docs->>'b')::numeric < 100;
SELECT * FROM ex WHERE (docs->>'b')::numeric > 9 AND (docs->>'b')::numeric < 100;

-- Leave 'a' unchanged but modify 'b' to a value within the index predicate.
-- This represents a change for field 'b' from unindexed to indexed and so
-- this should not take the HOT path.
UPDATE ex SET docs = jsonb_build_object('a', 0, 'b', 10) WHERE (docs->>'a')::numeric = 0;
SELECT pg_stat_get_xact_tuples_hot_updated('ex'::regclass); -- expect: 1 no new HOT updates
-- Let's check to make sure that the index contains the new value of 'b'
EXPLAIN (COSTS OFF) SELECT * FROM ex WHERE (docs->>'b')::numeric > 9 AND (docs->>'b')::numeric < 100;
SELECT * FROM ex WHERE (docs->>'b')::numeric > 9 AND (docs->>'b')::numeric < 100;

-- This update modifies the value of 'a', an indexed field, so it also cannot
-- be a HOT update.
UPDATE ex SET docs = jsonb_build_object('a', 1, 'b', 10) WHERE (docs->>'b')::numeric = 10;
SELECT pg_stat_get_xact_tuples_hot_updated('ex'::regclass); -- expect: 1 no new HOT updates

-- This update changes both 'a' and 'b' to new values that require index updates,
-- this cannot use the HOT path.
UPDATE ex SET docs = jsonb_build_object('a', 2, 'b', 12) WHERE (docs->>'b')::numeric = 10;
SELECT pg_stat_get_xact_tuples_hot_updated('ex'::regclass); -- expect: 1 no new HOT updates
-- Let's check to make sure that the index contains the new value of 'b'
EXPLAIN (COSTS OFF) SELECT * FROM ex WHERE (docs->>'b')::numeric > 9 AND (docs->>'b')::numeric < 100;
SELECT * FROM ex WHERE (docs->>'b')::numeric > 9 AND (docs->>'b')::numeric < 100;

-- This update changes 'b' to a value outside its predicate requiring that
-- we remove it from the index.  That's a transition that can't be done
-- during a HOT update.
UPDATE ex SET docs = jsonb_build_object('a', 2, 'b', 1) WHERE (docs->>'b')::numeric = 12;
SELECT pg_stat_get_xact_tuples_hot_updated('ex'::regclass); -- expect: 1 no new HOT updates
-- Let's check to make sure that the index no longer contains the value of 'b'
EXPLAIN (COSTS OFF) SELECT * FROM ex WHERE (docs->>'b')::numeric > 9 AND (docs->>'b')::numeric < 100;
SELECT * FROM ex WHERE (docs->>'b')::numeric > 9 AND (docs->>'b')::numeric < 100;

-- Disable expression checks.
ALTER TABLE ex SET (expression_checks = false);
SELECT reloptions FROM pg_class WHERE relname = 'ex';

-- This update changes 'b' to a value within its predicate just like it
-- previous value, which would allow for a HOT update but with expression
-- checks disabled we can't determine that so this should not be a HOT update.
UPDATE ex SET docs = jsonb_build_object('a', 2, 'b', 2) WHERE (docs->>'b')::numeric = 1;
SELECT pg_stat_get_xact_tuples_hot_updated('ex'::regclass); -- expect: 1 no new HOT updates


-- Let's make sure we're recording HOT updates for our 'ex' relation properly in the system
-- table pg_stat_user_tables.  Note that statistics are stored within a transaction context
-- first (xact) and then later into the global statistics for a relation, so first we need
-- to ensure pending stats are flushed.
SELECT pg_stat_force_next_flush();
SELECT
    c.relname AS table_name,
    -- Transaction statistics
    pg_stat_get_xact_tuples_updated(c.oid) AS xact_updates,
    pg_stat_get_xact_tuples_hot_updated(c.oid) AS xact_hot_updates,
    ROUND((
        pg_stat_get_xact_tuples_hot_updated(c.oid)::float /
        NULLIF(pg_stat_get_xact_tuples_updated(c.oid), 0) * 100
    )::numeric, 2) AS xact_hot_update_percentage,
    -- Cumulative statistics
    s.n_tup_upd AS total_updates,
    s.n_tup_hot_upd AS hot_updates,
    ROUND((
        s.n_tup_hot_upd::float /
        NULLIF(s.n_tup_upd, 0) * 100
    )::numeric, 2) AS total_hot_update_percentage
FROM pg_class c
LEFT JOIN pg_stat_user_tables s ON c.relname = s.relname
WHERE c.relname = 'ex'
AND c.relnamespace = 'public'::regnamespace;
-- expect: 5 xact updates with 1 xact hot update and no cumulative updates as yet

SET SESSION enable_seqscan = ON;
SET SESSION enable_bitmapscan = ON;

DROP TABLE ex;

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
SELECT pg_stat_get_xact_tuples_hot_updated('users'::regclass); -- expect: 0 no new HOT updates

-- Should succeed because the email column is not being updated and should go HOT.
UPDATE users SET name = 'foo' WHERE email = 'user1@example.com';
SELECT pg_stat_get_xact_tuples_hot_updated('users'::regclass); -- expect: 1 a single new HOT update

-- Create a partial index on the email column, updates
CREATE INDEX idx_users_email_no_example ON users (lower(email)) WHERE lower(email) LIKE '%@example.com%';

-- An update that changes the email column but not the indexed portion of it and falls outside the constraint.
-- Shouldn't be a HOT update because of the exclusion constraint.
UPDATE users SET email = 'you+2@domain.com' WHERE name = 'you';
SELECT pg_stat_get_xact_tuples_hot_updated('users'::regclass); -- expect: 1 no new HOT updates

-- An update that changes the email column but not the indexed portion of it and falls within the constraint.
-- Again, should fail constraint and fail to be a HOT update.
UPDATE users SET email = 'taken@domain.com' WHERE name = 'you';
SELECT pg_stat_get_xact_tuples_hot_updated('users'::regclass); -- expect: 1 no new HOT updates

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
SELECT pg_stat_get_xact_tuples_hot_updated('events'::regclass); -- expect: 0 no new HOT updates

-- Update the first event to not overlap with the second, again not HOT due to the constraint.
UPDATE events SET event_time = '["2023-01-01 22:00:00", "2023-01-01 22:45:00"]' WHERE id = 1;
SELECT pg_stat_get_xact_tuples_hot_updated('events'::regclass); -- expect: 0 no new HOT updates

-- Update the first event to not overlap with the second, this time we're HOT because we don't overlap with the constraint.
UPDATE events SET name = 'new name here' WHERE id = 1;
SELECT pg_stat_get_xact_tuples_hot_updated('events'::regclass); -- expect: 1 one new HOT update

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
SELECT pg_stat_get_xact_tuples_hot_updated('ex'::regclass); -- expect: 1 one new HOT update
SELECT * FROM ex;

-- Update att1 and att2, only one is BRIN/summarizing, this should NOT be a HOT update.
UPDATE ex SET att1 = att1 || '{"data": "howdy"}', att2 = 'special, so special';
SELECT pg_stat_get_xact_tuples_hot_updated('ex'::regclass); -- expect: 1 no new HOT updates
SELECT * FROM ex;

-- Update att2, att3, and att4 all are BRIN/summarizing indexes, this should be a HOT update
-- and yet still update all three summarizing indexes.
UPDATE ex SET att2 = 'a', att3 = 'b', att4 = 'c';
SELECT pg_stat_get_xact_tuples_hot_updated('ex'::regclass); -- expect: 2 with one new HOT update
SELECT * FROM ex;

-- Update att1, att2, and att3 all modified values are BRIN/summarizing indexes, this should be a HOT update
-- and yet still update all three summarizing indexes.
UPDATE ex SET att1 = '{"data": "howdy"}', att2 = 'd', att3 = 'e';
SELECT pg_stat_get_xact_tuples_hot_updated('ex'::regclass); -- expect: 3 with one new HOT update
SELECT * FROM ex;

DROP TABLE ex;

-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
-- A test to ensure that summarizing indexes are not updated when they don't
-- change, but are updated when they do while not prefent HOT updates.
CREATE TABLE ex (att1 JSONB, att2 text) WITH (fillfactor = 60);
CREATE INDEX ex_expr1_idx ON ex USING btree((att1->'data'));
CREATE INDEX ex_sumr1_idx ON ex USING BRIN(att2);
INSERT INTO ex VALUES ('{"data": []}', 'nothing special');
INSERT INTO ex VALUES ('{"data": []}', 'nothing special');

-- Update the unindexed value of att1, this should be a HOT update and not
-- update the summarizing index.
UPDATE ex SET att1 = att1 || '{"status": "checkmate"}';
SELECT pg_stat_get_xact_tuples_hot_updated('ex'::regclass); -- expect: 2 one new HOT update

-- Update the indexed value of att2, a summarized value, this is a summarized
-- only update and should use the HOT path while still triggering an update to
-- the summarizing BRIN index.
UPDATE ex SET att2 = 'special indeed';
SELECT pg_stat_get_xact_tuples_hot_updated('ex'::regclass); -- expect: 4 one new HOT update

-- Update to att1 doesn't change the indexed value while the update to att2 does,
-- this again is a summarized only update and should use the HOT path as well as
-- trigger an update to the BRIN index.
UPDATE ex SET att1 = att1 || '{"status": "checkmate!"}', att2 = 'special, so special';
SELECT pg_stat_get_xact_tuples_hot_updated('ex'::regclass); -- expect: 6 one new HOT update

-- This updates both indexes, the expression index on att1 and the summarizing
-- index on att2.  This should not be a HOT update because there are modified
-- indexes and only some are summarized, not all.  This should force all
-- indexes to be updated.
UPDATE ex SET att1 = att1 || '{"data": [1,2,3]}', att2 = 'do you want to play a game?';
SELECT pg_stat_get_xact_tuples_hot_updated('ex'::regclass); -- expect: 6 both indexes updated

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
SELECT pg_stat_get_xact_tuples_hot_updated('my_table'::regclass);

-- Update 2
UPDATE my_table SET custom_val = ROW(0)::my_custom_type WHERE custom_val < ROW(3)::my_custom_type;
SELECT pg_stat_get_xact_tuples_hot_updated('my_table'::regclass);

-- Update 3
UPDATE my_table SET custom_val = ROW(6)::my_custom_type WHERE id = 3;
SELECT pg_stat_get_xact_tuples_hot_updated('my_table'::regclass);

-- Update 4
UPDATE my_table SET id = 5 WHERE id = 1;
SELECT pg_stat_get_xact_tuples_hot_updated('my_table'::regclass);

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
