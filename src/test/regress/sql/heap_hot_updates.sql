SHOW enable_expression_checks;

-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
-- This table will have two columns and two indexes, one on the primary key
-- id and one on the expression (info->>'name').  That means that the indexed
-- attributes are 'id' and 'info'.
create table keyvalue(id integer primary key, info jsonb);
create index nameindex on keyvalue((info->>'name'));
insert into keyvalue values (1, '{"name": "john", "data": "some data"}');

-- Disable the GUC that allows us to skip expression checks.
SET enable_expression_checks = 'off';

-- The indexed attribute "name" with value "john" is unchanged, don't expect a HOT update.
update keyvalue set info='{"name": "john", "data": "something else"}' where id=1;
select pg_stat_get_xact_tuples_hot_updated('keyvalue'::regclass); -- expect: 0 row

-- Re-enable the GUC so that we don't skip expression checks.
SET enable_expression_checks = 'on';

-- The indexed attribute "name" with value "john" is unchanged, expect a HOT update.
update keyvalue set info='{"name": "john", "data": "some other data"}' where id=1;
select pg_stat_get_xact_tuples_hot_updated('keyvalue'::regclass); -- expect: 1 row

-- The following update changes the indexed attribute "name", this should not be a HOT update.
update keyvalue set info='{"name": "smith", "data": "some other data"}' where id=1;
select pg_stat_get_xact_tuples_hot_updated('keyvalue'::regclass); -- expect: 1 row, no new HOT updates

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
-- The table has one column _v and two indexes.  They are both expression
-- indexes referencing the same column attribute (_v) but one is a partial
-- index.
CREATE TABLE public.ex (_v JSONB) WITH (fillfactor = 60);
INSERT INTO ex (_v) VALUES ('{"a": 0, "b": 0}');
INSERT INTO ex (_v) SELECT jsonb_build_object('b', n) FROM generate_series(100, 10000) as n;
CREATE INDEX idx_ex_a ON ex ((_v->>'a'));
CREATE INDEX idx_ex_b ON ex ((_v->>'b')) WHERE (_v->>'b')::numeric > 9;

-- We're using BTREE indexes and for this test we want to make sure that they remain
-- in sync with changes to our relation.  Force the choice of index scans below so
-- that we know we're checking the index's understanding of what values should be
-- in the index or not.
SET SESSION enable_seqscan = OFF;
SET SESSION enable_bitmapscan = OFF;

-- Leave 'a' unchanged but modify 'b' to a value outside of the index predicate.
-- This should be a HOT update because neither index is changed.
UPDATE ex SET _v = jsonb_build_object('a', 0, 'b', 1) WHERE (_v->>'a')::numeric = 0;
SELECT pg_stat_get_xact_tuples_hot_updated('ex'::regclass); -- expect: 1 row
-- Let's check to make sure that the index does not contain a value for 'b'
EXPLAIN (COSTS OFF) SELECT * FROM ex WHERE (_v->>'b')::numeric > 9 AND (_v->>'b')::numeric < 100;
SELECT * FROM ex WHERE (_v->>'b')::numeric > 9 AND (_v->>'b')::numeric < 100;

-- Leave 'a' unchanged but modify 'b' to a value within the index predicate.
-- This represents a change for field 'b' from unindexed to indexed and so
-- this should not take the HOT path.
UPDATE ex SET _v = jsonb_build_object('a', 0, 'b', 10) WHERE (_v->>'a')::numeric = 0;
SELECT pg_stat_get_xact_tuples_hot_updated('ex'::regclass); -- expect: 1 row, no new HOT updates
-- Let's check to make sure that the index contains the new value of 'b'
EXPLAIN (COSTS OFF) SELECT * FROM ex WHERE (_v->>'b')::numeric > 9 AND (_v->>'b')::numeric < 100;
SELECT * FROM ex WHERE (_v->>'b')::numeric > 9 AND (_v->>'b')::numeric < 100;

-- This update modifies the value of 'a', an indexed field, so it also cannot
-- be a HOT update.
UPDATE ex SET _v = jsonb_build_object('a', 1, 'b', 10) WHERE (_v->>'b')::numeric = 10;
SELECT pg_stat_get_xact_tuples_hot_updated('ex'::regclass); -- expect: 1 row, no new HOT updates

-- This update changes both 'a' and 'b' to new values that require index updates,
-- this cannot use the HOT path.
UPDATE ex SET _v = jsonb_build_object('a', 2, 'b', 12) WHERE (_v->>'b')::numeric = 10;
SELECT pg_stat_get_xact_tuples_hot_updated('ex'::regclass); -- expect: 1 row, no new HOT updates
-- Let's check to make sure that the index contains the new value of 'b'
EXPLAIN (COSTS OFF) SELECT * FROM ex WHERE (_v->>'b')::numeric > 9 AND (_v->>'b')::numeric < 100;
SELECT * FROM ex WHERE (_v->>'b')::numeric > 9 AND (_v->>'b')::numeric < 100;

-- This update changes 'b' to a value outside its predicate requiring that
-- we remove it from the index.  That's a transition that can't be done
-- during a HOT update.
UPDATE ex SET _v = jsonb_build_object('a', 2, 'b', 1) WHERE (_v->>'b')::numeric = 12;
SELECT pg_stat_get_xact_tuples_hot_updated('ex'::regclass); -- expect: 1 row, no new HOT updates
-- Let's check to make sure that the index no longer contains the value of 'b'
EXPLAIN (COSTS OFF) SELECT * FROM ex WHERE (_v->>'b')::numeric > 9 AND (_v->>'b')::numeric < 100;
SELECT * FROM ex WHERE (_v->>'b')::numeric > 9 AND (_v->>'b')::numeric < 100;

-- Let's make sure we're recording HOT updates for our 'ex' relation properly in the system
-- table pg_stat_user_tables.  Note that statistics are stored within a transaction context
-- first (xact) and then later into the global statistics for a relation.
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

DROP TABLE public.ex;

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
SELECT pg_stat_get_xact_tuples_hot_updated('users'::regclass); -- expect: 0 rows, no new HOT updates

-- Should succeed because the email column is not being updated and should go HOT.
UPDATE users SET name = 'foo' WHERE email = 'user1@example.com';
SELECT pg_stat_get_xact_tuples_hot_updated('users'::regclass); -- expect: 1 row, a single new HOT update

-- Create a partial index on the email column, updates 
CREATE INDEX idx_users_email_no_example ON users (lower(email)) WHERE lower(email) LIKE '%@example.com%';

-- An update that changes the email column but not the indexed portion of it and falls outside the constraint.
-- Shouldn't be a HOT update because of the exclusion constraint.
UPDATE users SET email = 'you+2@domain.com' WHERE name = 'you';
SELECT pg_stat_get_xact_tuples_hot_updated('users'::regclass); -- expect: 1 row, no new HOT updates

-- An update that changes the email column but not the indexed portion of it and falls within the constraint.
-- Again, should fail constraint and fail to be a HOT update.
UPDATE users SET email = 'taken@domain.com' WHERE name = 'you';
SELECT pg_stat_get_xact_tuples_hot_updated('users'::regclass); -- expect: 1 row, no new HOT updates

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
SELECT pg_stat_get_xact_tuples_hot_updated('events'::regclass); -- expect: 0 row, no new HOT updates

-- Update the first event to not overlap with the second, again not HOT due to the constraint.
UPDATE events SET event_time = '["2023-01-01 22:00:00", "2023-01-01 22:45:00"]' WHERE id = 1;
SELECT pg_stat_get_xact_tuples_hot_updated('events'::regclass); -- expect: 0 row, no new HOT updates

-- Update the first event to not overlap with the second, this time we're HOT because we don't overlap with the constraint.
UPDATE events SET name = 'new name here' WHERE id = 1;
SELECT pg_stat_get_xact_tuples_hot_updated('events'::regclass); -- expect: 1 row, one new HOT update

DROP TABLE events;
