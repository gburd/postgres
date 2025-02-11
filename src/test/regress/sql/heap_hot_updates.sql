SHOW enable_expression_checks;

-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
-- This table will have two columns and two indexes, one on the primary key
-- id and one on the expression (info->>'name').  That means that the indexed
-- attributes are 'id' and 'info'.
create table keyvalue(id integer primary key, info jsonb);
create index nameindex on keyvalue((info->>'name'));
insert into keyvalue values (1, '{"name": "john", "data": "some data"}');

-- None of the indexes require changes with this update, expect a HOT update.
update keyvalue set info='{"name": "john", "data": "some other data"}' where id=1;
select pg_stat_get_xact_tuples_hot_updated('keyvalue'::regclass); -- expect: 1 row

-- The following update changes the indexed attribute "name", but not the
-- primary key index, this is a partial HOT (PHOT) update as some but not
-- all indexes require updates.
update keyvalue set info='{"name": "smith", "data": "some other data"}' where id=1;
select pg_stat_get_xact_tuples_hot_updated('keyvalue'::regclass); -- expect: 2 rows

-- This update changes the primary key index but not the other index on name
-- within the info column, it is a PHOT update (1 of 2 indexes updated).
update keyvalue set id=100, info='{"name": "smith", "data": "some more data"}' where id=1;
select pg_stat_get_xact_tuples_hot_updated('keyvalue'::regclass); -- expect: 3 rows

-- This update changes values in both indexes, so not HOT or PHOT because
-- we're updating all indexed values.
update keyvalue set id=1, info='{"name": "john", "data": "some more data"}' where id=100;
select pg_stat_get_xact_tuples_hot_updated('keyvalue'::regclass); -- expect: 3 rows
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
-- change the content of the info column and so should be PHOT because one of
-- the three indexes changed as a result of the update so only one should be
-- updated with a new index entry.
update keyvalue set info='{"name": "john", "data": "some other data"}' where id=1;
select pg_stat_get_xact_tuples_hot_updated('keyvalue'::regclass); -- expect: 1 row

-- This update changes the primary key index but not the other index on name
-- within the info column, it is a PHOT update (1 of 3 indexes updated).
update keyvalue set id=100, info='{"name": "john", "data": "some more data here"}' where id=1;
select pg_stat_get_xact_tuples_hot_updated('keyvalue'::regclass); -- expect: 2 rows

-- This update changes the primary key index and the index on name within
-- the info column, it is not HOT because it updated all indexes.
update keyvalue set id=1, info='{"name": "smith", "data": "some more data"}' where id=100;
select pg_stat_get_xact_tuples_hot_updated('keyvalue'::regclass); -- expect: 2 rows
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

-- Leave 'a' unchanged but modify 'b' to a value outside of the index predicate.
-- This should be a HOT update because neither index is changed.
UPDATE ex SET _v = jsonb_build_object('a', 0, 'b', 1) WHERE (_v->>'a')::numeric = 0;
SELECT pg_stat_get_xact_tuples_hot_updated('ex'::regclass); -- expect: 1 row
-- Let's check to make sure that the index does not contain a value for 'b'
EXPLAIN (COSTS OFF) SELECT * FROM ex WHERE (_v->>'b')::numeric > 9 AND (_v->>'b')::numeric < 100;
SELECT * FROM ex WHERE (_v->>'b')::numeric > 9 AND (_v->>'b')::numeric < 100;
-- Let's check again, using the BTREE index we know exists
SET SESSION enable_seqscan = OFF;
SET SESSION enable_bitmapscan = OFF;
EXPLAIN (COSTS OFF) SELECT * FROM ex WHERE (_v->>'b')::numeric > 9 AND (_v->>'b')::numeric < 100;
SELECT * FROM ex WHERE (_v->>'b')::numeric > 9 AND (_v->>'b')::numeric < 100;
SET SESSION enable_seqscan = ON;
SET SESSION enable_bitmapscan = ON;

-- Leave 'a' unchanged but modify 'b' to a value within the index predicate.
-- This represents a change for field 'b' from unindexed to indexed and so
-- this should take the PHOT path.
UPDATE ex SET _v = jsonb_build_object('a', 0, 'b', 10) WHERE (_v->>'a')::numeric = 0;
VACUUM ANALYZE ex;
SELECT pg_stat_get_xact_tuples_hot_updated('ex'::regclass); -- expect: 2 rows
-- Let's check to make sure that the index contains the new value of 'b'
EXPLAIN (COSTS OFF) SELECT * FROM ex WHERE (_v->>'b')::numeric > 9 AND (_v->>'b')::numeric < 100;
SELECT * FROM ex WHERE (_v->>'b')::numeric > 9 AND (_v->>'b')::numeric < 100;

-- This update modifies the value of 'a', an indexed field, so again this is
-- a PHOT update.
UPDATE ex SET _v = jsonb_build_object('a', 1, 'b', 10) WHERE (_v->>'b')::numeric = 10;
VACUUM ANALYZE ex;
SELECT pg_stat_get_xact_tuples_hot_updated('ex'::regclass); -- expect: 3 rows

-- This update changes both 'a' and 'b' to new values so all indexes require
-- updates, this should not use the HOT or PHOT path.
UPDATE ex SET _v = jsonb_build_object('a', 2, 'b', 12) WHERE (_v->>'b')::numeric = 10;
VACUUM ANALYZE ex;
SELECT pg_stat_get_xact_tuples_hot_updated('ex'::regclass); -- expect: 3 row, no new HOT updates
-- Let's check to make sure that the index contains the new value of 'b'
EXPLAIN (COSTS OFF) SELECT * FROM ex WHERE (_v->>'b')::numeric > 9 AND (_v->>'b')::numeric < 100;
SELECT * FROM ex WHERE (_v->>'b')::numeric > 9 AND (_v->>'b')::numeric < 100;

-- This update changes 'b' to a value outside its predicate requiring that
-- we remove it from the index.  That's a transition for the index and so
-- this should be a PHOT update.
UPDATE ex SET _v = jsonb_build_object('a', 2, 'b', 1) WHERE (_v->>'b')::numeric = 12;
VACUUM ANALYZE ex;
SELECT pg_stat_get_xact_tuples_hot_updated('ex'::regclass); -- expect: 4 rows
-- Let's check to make sure that the index no longer contains the value of 'b'
EXPLAIN (COSTS OFF) SELECT * FROM ex WHERE (_v->>'b')::numeric > 9 AND (_v->>'b')::numeric < 100;
SELECT * FROM ex WHERE (_v->>'b')::numeric > 9 AND (_v->>'b')::numeric < 100;

VACUUM ANALYZE ex;

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
-- expect: 5 xact updates with 4 xact hot update

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
