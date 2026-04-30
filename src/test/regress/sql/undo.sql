--
-- Tests for UNDO logging (enable_undo storage parameter)
--

-- ================================================================
-- Section 1: enable_undo storage parameter basics
-- ================================================================

-- Create table with UNDO enabled
CREATE TABLE undo_basic (id int, data text) WITH (enable_undo = on);

-- Verify the storage parameter is set
SELECT reloptions FROM pg_class WHERE oid = 'undo_basic'::regclass;

-- Create table without UNDO (default)
CREATE TABLE undo_default (id int, data text);
SELECT reloptions FROM pg_class WHERE oid = 'undo_default'::regclass;

-- ALTER TABLE to enable UNDO
ALTER TABLE undo_default SET (enable_undo = on);
SELECT reloptions FROM pg_class WHERE oid = 'undo_default'::regclass;

-- ALTER TABLE to disable UNDO
ALTER TABLE undo_default SET (enable_undo = off);
SELECT reloptions FROM pg_class WHERE oid = 'undo_default'::regclass;

-- Boolean-style: specifying name only enables it
ALTER TABLE undo_default SET (enable_undo);
SELECT reloptions FROM pg_class WHERE oid = 'undo_default'::regclass;

-- Reset
ALTER TABLE undo_default RESET (enable_undo);
SELECT reloptions FROM pg_class WHERE oid = 'undo_default'::regclass AND reloptions IS NULL;

-- Invalid values for enable_undo
CREATE TABLE undo_bad (id int) WITH (enable_undo = 'string');
CREATE TABLE undo_bad (id int) WITH (enable_undo = 42);

-- ================================================================
-- Section 2: Basic DML with UNDO-enabled table
-- ================================================================

-- INSERT
INSERT INTO undo_basic VALUES (1, 'first');
INSERT INTO undo_basic VALUES (2, 'second');
INSERT INTO undo_basic VALUES (3, 'third');
SELECT * FROM undo_basic ORDER BY id;

-- UPDATE
UPDATE undo_basic SET data = 'updated_first' WHERE id = 1;
SELECT * FROM undo_basic ORDER BY id;

-- DELETE
DELETE FROM undo_basic WHERE id = 2;
SELECT * FROM undo_basic ORDER BY id;

-- Verify correct final state
SELECT count(*) FROM undo_basic;

-- ================================================================
-- Section 3: Transaction rollback with UNDO
-- ================================================================

-- INSERT then rollback
BEGIN;
INSERT INTO undo_basic VALUES (10, 'will_rollback');
SELECT count(*) FROM undo_basic WHERE id = 10;
ROLLBACK;
SELECT count(*) FROM undo_basic WHERE id = 10;

-- DELETE then rollback
BEGIN;
DELETE FROM undo_basic WHERE id = 1;
SELECT count(*) FROM undo_basic WHERE id = 1;
ROLLBACK;
SELECT count(*) FROM undo_basic WHERE id = 1;

-- UPDATE then rollback
BEGIN;
UPDATE undo_basic SET data = 'temp_update' WHERE id = 3;
SELECT data FROM undo_basic WHERE id = 3;
ROLLBACK;
SELECT data FROM undo_basic WHERE id = 3;

-- ================================================================
-- Section 4: Subtransactions with UNDO
-- ================================================================

BEGIN;
INSERT INTO undo_basic VALUES (20, 'parent_insert');
SAVEPOINT sp1;
INSERT INTO undo_basic VALUES (21, 'child_insert');
ROLLBACK TO sp1;
-- child_insert should be gone, parent_insert should remain
SELECT id, data FROM undo_basic WHERE id IN (20, 21) ORDER BY id;
COMMIT;
SELECT id, data FROM undo_basic WHERE id IN (20, 21) ORDER BY id;

-- Nested savepoints
BEGIN;
INSERT INTO undo_basic VALUES (30, 'level0');
SAVEPOINT sp1;
INSERT INTO undo_basic VALUES (31, 'level1');
SAVEPOINT sp2;
INSERT INTO undo_basic VALUES (32, 'level2');
ROLLBACK TO sp2;
-- level2 gone, level0 and level1 remain
SELECT id, data FROM undo_basic WHERE id IN (30, 31, 32) ORDER BY id;
ROLLBACK TO sp1;
-- level1 also gone, only level0 remains
SELECT id, data FROM undo_basic WHERE id IN (30, 31, 32) ORDER BY id;
COMMIT;
SELECT id, data FROM undo_basic WHERE id IN (30, 31, 32) ORDER BY id;

-- ================================================================
-- Section 5: System catalog protection
-- ================================================================

-- Attempting to set enable_undo on a system catalog should be silently
-- ignored (RelationHasUndo returns false for system relations).
-- We can't ALTER system catalogs directly, but we verify the protection
-- exists by checking that system tables never report enable_undo.
SELECT c.relname, c.reloptions
FROM pg_class c
WHERE c.relnamespace = 'pg_catalog'::regnamespace
  AND c.reloptions::text LIKE '%enable_undo%'
LIMIT 1;

-- ================================================================
-- Section 6: Mixed UNDO and non-UNDO tables
-- ================================================================

CREATE TABLE no_undo_table (id int, data text);
INSERT INTO no_undo_table VALUES (1, 'no_undo');

BEGIN;
INSERT INTO undo_basic VALUES (40, 'undo_row');
INSERT INTO no_undo_table VALUES (2, 'no_undo_row');
ROLLBACK;

-- Both inserts should be rolled back (standard PostgreSQL behavior)
SELECT count(*) FROM undo_basic WHERE id = 40;
SELECT count(*) FROM no_undo_table WHERE id = 2;

-- ================================================================
-- Section 7: UNDO with TRUNCATE
-- ================================================================

CREATE TABLE undo_trunc (id int) WITH (enable_undo = on);
INSERT INTO undo_trunc SELECT generate_series(1, 10);
SELECT count(*) FROM undo_trunc;

TRUNCATE undo_trunc;
SELECT count(*) FROM undo_trunc;

-- Re-insert after truncate
INSERT INTO undo_trunc VALUES (100);
SELECT * FROM undo_trunc;

-- ================================================================
-- Section 8: GUC validation - undo_buffer_size
-- ================================================================

-- undo_buffer_size is a POSTMASTER context GUC, so we can SHOW it
-- but cannot SET it at runtime.
SHOW undo_buffer_size;

-- ================================================================
-- Section 9: UNDO with various data types
-- ================================================================

CREATE TABLE undo_types (
    id serial,
    int_val int,
    text_val text,
    float_val float8,
    bool_val boolean,
    ts_val timestamp
) WITH (enable_undo = on);

INSERT INTO undo_types (int_val, text_val, float_val, bool_val, ts_val)
VALUES (42, 'hello world', 3.14, true, '2024-01-01 12:00:00');

BEGIN;
UPDATE undo_types SET text_val = 'changed', float_val = 2.71 WHERE id = 1;
SELECT text_val, float_val FROM undo_types WHERE id = 1;
ROLLBACK;
SELECT text_val, float_val FROM undo_types WHERE id = 1;

-- ================================================================
-- Section 10: TOAST with UNDO (>2KB values)
-- ================================================================

CREATE TABLE undo_toast (id int, big_val text) WITH (enable_undo = on);

-- Insert a row with a large value that will be TOASTed.
INSERT INTO undo_toast VALUES (1, repeat('A', 10000));
SELECT id, length(big_val) FROM undo_toast;

-- UPDATE with TOASTed value, then rollback.
BEGIN;
UPDATE undo_toast SET big_val = repeat('B', 20000) WHERE id = 1;
SELECT length(big_val) FROM undo_toast WHERE id = 1;
ROLLBACK;

-- Original TOASTed value should be restored.
SELECT id, length(big_val), substr(big_val, 1, 5) FROM undo_toast WHERE id = 1;

-- DELETE with TOASTed value, then rollback.
BEGIN;
DELETE FROM undo_toast WHERE id = 1;
SELECT count(*) FROM undo_toast;
ROLLBACK;

SELECT id, length(big_val) FROM undo_toast;

-- ================================================================
-- Section 11: Deep subtransaction nesting with UNDO
-- ================================================================

CREATE TABLE undo_deep_sub (id int, val text) WITH (enable_undo = on);

-- Nest 20 savepoints deep with modifications at each level.
BEGIN;
INSERT INTO undo_deep_sub VALUES (1, 'base');
DO $$
DECLARE
    i int;
BEGIN
    FOR i IN 1..20 LOOP
        EXECUTE format('SAVEPOINT sp_%s', i);
        EXECUTE format(
            'INSERT INTO undo_deep_sub VALUES (%s, %L)',
            100 + i, 'level' || i
        );
    END LOOP;
    -- Roll back the inner 10 levels.
    FOR i IN REVERSE 20..11 LOOP
        EXECUTE format('ROLLBACK TO sp_%s', i);
    END LOOP;
END;
$$;

-- Should have: base row (1) + levels 1-10 (100+1 through 100+10).
SELECT count(*) FROM undo_deep_sub;
COMMIT;

SELECT id, val FROM undo_deep_sub ORDER BY id;

-- ================================================================
-- Section 12: HOT update with UNDO
-- ================================================================

-- Create a table where HOT updates are possible (no index on 'val').
CREATE TABLE undo_hot (id int PRIMARY KEY, val text, counter int)
  WITH (enable_undo = on, fillfactor = 50);

INSERT INTO undo_hot SELECT g, 'initial', 0 FROM generate_series(1, 20) g;

-- Update a non-indexed column (should be a HOT update).
BEGIN;
UPDATE undo_hot SET counter = 1 WHERE id = 5;
UPDATE undo_hot SET counter = 2 WHERE id = 5;
SELECT counter FROM undo_hot WHERE id = 5;
ROLLBACK;

-- After rollback, counter should be 0 (original value).
SELECT counter FROM undo_hot WHERE id = 5;

-- Committed HOT update should persist.
UPDATE undo_hot SET counter = 99 WHERE id = 10;
SELECT counter FROM undo_hot WHERE id = 10;

-- ================================================================
-- Section 13: Large batch operations with UNDO write buffer
-- ================================================================

CREATE TABLE undo_batch (id int, val text) WITH (enable_undo = on);

-- Large insert should use the UNDO write buffer.
INSERT INTO undo_batch SELECT g, 'row' || g FROM generate_series(1, 5000) g;
SELECT count(*) FROM undo_batch;

-- Large delete in a transaction, then rollback.
BEGIN;
DELETE FROM undo_batch WHERE id <= 2500;
SELECT count(*) FROM undo_batch;
ROLLBACK;

SELECT count(*) FROM undo_batch;

-- Large update in a transaction, then rollback.
BEGIN;
UPDATE undo_batch SET val = 'changed' WHERE id > 2500;
ROLLBACK;

SELECT count(*) FROM undo_batch WHERE val = 'changed';

-- ================================================================
-- Section 14: UNDO with indexes (B-tree)
-- ================================================================

CREATE TABLE undo_idx (id int PRIMARY KEY, val text, num int)
  WITH (enable_undo = on);
CREATE INDEX undo_idx_num ON undo_idx (num);

INSERT INTO undo_idx SELECT g, 'v' || g, g * 10 FROM generate_series(1, 100) g;

-- Index scan should work.
SET enable_seqscan = off;
SELECT id, num FROM undo_idx WHERE num = 500;
RESET enable_seqscan;

-- Rollback of INSERT should leave indexes consistent.
BEGIN;
INSERT INTO undo_idx VALUES (200, 'new', 2000);
-- Verify via index scan.
SET enable_seqscan = off;
SELECT id FROM undo_idx WHERE num = 2000;
RESET enable_seqscan;
ROLLBACK;

-- The rolled-back row should not be findable via index.
SET enable_seqscan = off;
SELECT count(*) FROM undo_idx WHERE num = 2000;
RESET enable_seqscan;

-- Total count via seqscan should match.
SELECT count(*) FROM undo_idx;

-- ================================================================
-- Section 15: enable_undo = min mode is rejected
-- ================================================================

-- min mode has been removed; verify it is rejected
CREATE TABLE undo_min_rejected (id int, data text) WITH (enable_undo = min);

-- Boolean aliases should map correctly
CREATE TABLE undo_bool_test (id int) WITH (enable_undo = true);
SELECT reloptions FROM pg_class WHERE oid = 'undo_bool_test'::regclass;
DROP TABLE undo_bool_test;

CREATE TABLE undo_bool_test (id int) WITH (enable_undo = false);
SELECT reloptions FROM pg_class WHERE oid = 'undo_bool_test'::regclass;
DROP TABLE undo_bool_test;

-- ================================================================
-- Cleanup
-- ================================================================

DROP TABLE undo_basic;
DROP TABLE undo_default;
DROP TABLE no_undo_table;
DROP TABLE undo_trunc;
DROP TABLE undo_types;
DROP TABLE undo_toast;
DROP TABLE undo_deep_sub;
DROP TABLE undo_hot;
DROP TABLE undo_batch;
DROP TABLE undo_idx;
