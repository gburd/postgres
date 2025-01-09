-- Create a function to measure HOT updates
CREATE OR REPLACE FUNCTION check_hot_updates(
    expected INT,
    p_table_name TEXT DEFAULT 't',
    p_schema_name TEXT DEFAULT current_schema()
)
RETURNS TABLE (
    total_updates BIGINT,
    hot_updates BIGINT,
    hot_update_percentage NUMERIC,
    matches_expected BOOLEAN
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
        v_updates::BIGINT as total_updates,
        v_hot_updates::BIGINT as hot_updates,
        CASE
            WHEN v_updates > 0 THEN
                ROUND((v_hot_updates::numeric / v_updates::numeric * 100)::numeric, 2)
            ELSE 0
        END as hot_update_percentage,
        (v_hot_updates = expected)::BOOLEAN as matches_expected;
END;
$$;

-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
CREATE TABLE t(docs JSONB) WITH (autovacuum_enabled = off, fillfactor = 70);
INSERT INTO t VALUES ('{"name": "john", "id": "1"}');

SET SESSION enable_seqscan = OFF;
SET SESSION enable_bitmapscan = OFF;

-- No indexes on the relation yet, this update should not be HOT.
UPDATE t SET docs='{"name": "john", "id": "2"}';
SELECT * FROM check_hot_updates(0);

-- Add an expression index.
CREATE INDEX t_docs_name_idx ON t((docs->>'name'));

-- Update without changing the indexed value, should be HOT.
UPDATE t SET docs='{"name": "john", "id": "3"}';
SELECT * FROM check_hot_updates(1);

-- See what we find using the index.
EXPLAIN (COSTS OFF) SELECT * FROM t WHERE (docs->>'name') = 'john';
SELECT * FROM t WHERE (docs->>'name') = 'john';

-- Disable potentially expensive expression checks on the relation.
ALTER TABLE t SET (expression_checks = false);
SELECT reloptions FROM pg_class WHERE relname = 't';

-- All indexes will appear to have changed, so not a HOT update.
UPDATE t SET docs='{"name": "john", "id": "4"}';
SELECT * FROM check_hot_updates(1);

-- See what we find using the index.
EXPLAIN (COSTS OFF) SELECT * FROM t WHERE (docs->>'name') = 'john';
SELECT * FROM t WHERE (docs->>'name') = 'john';

-- Re-enable expression checks on the relation.
ALTER TABLE t SET (expression_checks = true);
SELECT reloptions FROM pg_class WHERE relname = 't';

-- All indexes have changed, so not a HOT update.
UPDATE t SET docs='{"name": "bill", "id": "5"}';
SELECT * FROM check_hot_updates(1);

-- See what we find using the index.
EXPLAIN (COSTS OFF) SELECT * FROM t WHERE (docs->>'name') = 'bill';
SELECT * FROM t WHERE (docs->>'name') = 'bill';

-- Add a second index.
CREATE INDEX t_docs_id_idx ON t((docs->>'id'));

-- Disable potentially expensive expression checks on the relation.
ALTER TABLE t SET (expression_checks = false);
SELECT reloptions FROM pg_class WHERE relname = 't';

-- Without the ability to check expressions both indexes will
-- need to be updated.  Not PHOT where it could have been.
UPDATE t SET docs='{"name": "bill", "id": "6"}';
SELECT * FROM check_hot_updates(1);

-- See what we find using the name index.
EXPLAIN (COSTS OFF) SELECT * FROM t WHERE (docs->>'name') = 'bill';
SELECT * FROM t WHERE (docs->>'name') = 'bill';

-- See what we find using the new index on id.
EXPLAIN (COSTS OFF) SELECT * FROM t WHERE (docs->>'id') = '6';
SELECT * FROM t WHERE (docs->>'id') = '6';

-- Re-enable expression checks on the relation.
ALTER TABLE t SET (expression_checks = true);
SELECT reloptions FROM pg_class WHERE relname = 't';

--With checks re-enabled we'll note that only one indexed value
-- changed and the other didn't, so we can use the PHOT path.
UPDATE t SET docs='{"name": "bill", "id": "7"}';
SELECT * FROM check_hot_updates(2);

-- See what we find using the name index.
EXPLAIN (COSTS OFF) SELECT * FROM t WHERE (docs->>'name') = 'bill';
SELECT * FROM t WHERE (docs->>'name') = 'bill';

-- See what we find using the new index on id.
EXPLAIN (COSTS OFF) SELECT * FROM t WHERE (docs->>'id') = '7';
SELECT * FROM t WHERE (docs->>'id') = '7';

VACUUM t;
SELECT * FROM t;
DROP TABLE t;
SET SESSION enable_seqscan = ON;
SET SESSION enable_bitmapscan = ON;


-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
-- This table is the similar to the previous one but it has three indexes.
-- The index 'colindex' isn't an expression index, it indexes the entire value
-- in the docs column.  There are still only two indexed attributes for this
-- relation, the same two as before.  The presence of an index on the entire
-- value of the docs column should prevent HOT updates for any updates to any
-- portion of JSONB content in that column.
CREATE TABLE  t(id INT PRIMARY KEY, docs JSONB);
CREATE INDEX t_docs_name_idx ON t((docs->>'name'));
CREATE INDEX t_docs_idx ON t(docs);
INSERT INTO t VALUES (1, '{"name": "john", "data": "some data"}');

-- This update doesn't change the value of the expression index, but it does
-- change the content of the docs column and so should be PHOT because one of
-- the three indexes changed as a result of the update so only one should be
-- updated with a new index entry.
UPDATE t SET docs='{"name": "john", "data": "some other data"}' where id=1;
SELECT * FROM check_hot_updates(1);

-- This update changes the primary key index but not the other index on name
-- within the docs column, it is a PHOT update (1 of 3 indexes updated).
UPDATE t SET id=100, docs='{"name": "john", "data": "some more data here"}' WHERE id=1;
SELECT * FROM check_hot_updates(2);

-- This update changes the primary key index and the index on name within
-- the docs column, it is not HOT because it updated all three indexes.
UPDATE t SET id=1, docs='{"name": "smith", "data": "some more data"}' WHERE id=100;
SELECT * FROM check_hot_updates(2);

VACUUM t;
DROP TABLE t;

-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
-- This table has one column and two indexes.  They are both expression
-- indexes referencing the same column attribute (docs) but one is a partial
-- index.
CREATE TABLE t (docs JSONB) WITH (autovacuum_enabled = off, fillfactor = 60);
INSERT INTO t (docs) VALUES ('{"a": 0, "b": 0}');
INSERT INTO t (docs) SELECT jsonb_build_object('b', n) FROM generate_series(100, 10000) as n;
CREATE INDEX t_docs_a_idx ON t ((docs->>'a'));
CREATE INDEX t_docs_b_idx ON t ((docs->>'b')) WHERE (docs->>'b')::numeric > 9;

-- Leave 'a' unchanged but modify 'b' to a value outside of the index predicate.
-- This should be a HOT update because neither index is changed.
UPDATE t SET docs = jsonb_build_object('a', 0, 'b', 1) WHERE (docs->>'a')::numeric = 0;
SELECT * FROM check_hot_updates(1);

-- Let's check to make sure that the index does not contain a value for 'b'
EXPLAIN (COSTS OFF) SELECT * FROM t WHERE (docs->>'b')::numeric > 9 AND (docs->>'b')::numeric < 100;
SELECT * FROM t WHERE (docs->>'b')::numeric > 9 AND (docs->>'b')::numeric < 100;
-- Let's check again, using the BTREE index we know exists
SET SESSION enable_seqscan = OFF;
SET SESSION enable_bitmapscan = OFF;
EXPLAIN (COSTS OFF) SELECT * FROM t WHERE (docs->>'b')::numeric > 9 AND (docs->>'b')::numeric < 100;
SELECT * FROM t WHERE (docs->>'b')::numeric > 9 AND (docs->>'b')::numeric < 100;

-- Leave 'a' unchanged but modify 'b' to a value within the index predicate.
-- This represents a change for field 'b' from unindexed to indexed and so
-- this should take the PHOT path.
UPDATE t SET docs = jsonb_build_object('a', 0, 'b', 10) WHERE (docs->>'a')::numeric = 0;
VACUUM ANALYZE t;
SELECT * FROM check_hot_updates(2);

-- Let's check to make sure that the index contains the new value of 'b'
EXPLAIN (COSTS OFF) SELECT * FROM t WHERE (docs->>'b')::numeric > 9 AND (docs->>'b')::numeric < 100;
SELECT * FROM t WHERE (docs->>'b')::numeric > 9 AND (docs->>'b')::numeric < 100;

-- This update modifies the value of 'a', an indexed field, so again this is
-- a PHOT update.
UPDATE t SET docs = jsonb_build_object('a', 1, 'b', 10) WHERE (docs->>'b')::numeric = 10;
VACUUM ANALYZE t;
SELECT * FROM check_hot_updates(3);

-- This update changes both 'a' and 'b' to new values so all indexes require
-- updates, this should not use the HOT or PHOT path.
UPDATE t SET docs = jsonb_build_object('a', 2, 'b', 12) WHERE (docs->>'b')::numeric = 10;
VACUUM ANALYZE t;
SELECT * FROM check_hot_updates(3);

-- Let's check to make sure that the index contains the new value of 'b'
EXPLAIN (COSTS OFF) SELECT * FROM t WHERE (docs->>'b')::numeric > 9 AND (docs->>'b')::numeric < 100;
SELECT * FROM t WHERE (docs->>'b')::numeric > 9 AND (docs->>'b')::numeric < 100;

-- This update changes 'b' to a value outside its predicate requiring that
-- we remove it from the index.  That's a transition for the index and so
-- this should be a PHOT update.
UPDATE t SET docs = jsonb_build_object('a', 2, 'b', 1) WHERE (docs->>'b')::numeric = 12;
VACUUM ANALYZE t;
SELECT * FROM check_hot_updates(4);

-- Let's check to make sure that the index no longer contains the value of 'b'
EXPLAIN (COSTS OFF) SELECT * FROM t WHERE (docs->>'b')::numeric > 9 AND (docs->>'b')::numeric < 100;
SELECT * FROM t WHERE (docs->>'b')::numeric > 9 AND (docs->>'b')::numeric < 100;

VACUUM ANALYZE t;

SET SESSION enable_seqscan = ON;
SET SESSION enable_bitmapscan = ON;

DROP TABLE t;

-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
-- Tests to ensure that HOT updates are not performed when multiple indexed
-- attributes are updated.
CREATE TABLE t(a INT, b INT) WITH (autovacuum_enabled = off, fillfactor = 60);
SELECT reset_table_stats('t');
CREATE INDEX t_idx_a ON t(a);
CREATE INDEX t_idx_b ON t(abs(b));
INSERT INTO t VALUES (1, -1);

-- Both are updated, the second is an expression index with an unchanged
-- index value.  The change to the index on a should prevent HOT updates.
UPDATE t SET a = 2, b = 1 WHERE a = 1;
SELECT * FROM check_hot_updates(0, 't');

VACUUM ANALYZE t;

DROP TABLE t;


-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
-- Clean up
DROP FUNCTION check_hot_updates(int, text, text);
