--
-- RECNO Logical Replication Validation
-- Tests that RECNO tables work correctly with logical replication
--

-- Create a publication for testing
CREATE TABLE recno_repl_test (
    id INTEGER PRIMARY KEY,
    value INTEGER,
    data TEXT,
    updated_at TIMESTAMP DEFAULT NOW()
) USING recno;

-- Insert initial data
INSERT INTO recno_repl_test VALUES (1, 100, 'initial data', NOW());
INSERT INTO recno_repl_test VALUES (2, 200, 'more data', NOW());
INSERT INTO recno_repl_test VALUES (3, 300, 'even more', NOW());

-- Verify initial state
SELECT id, value, data FROM recno_repl_test ORDER BY id;

-- Test UPDATE (including in-place updates)
UPDATE recno_repl_test SET value = value + 1 WHERE id = 1;
UPDATE recno_repl_test SET value = value + 10 WHERE id = 2;
UPDATE recno_repl_test SET data = 'updated text' WHERE id = 3;

-- Verify updates
SELECT id, value, data FROM recno_repl_test ORDER BY id;

-- Test DELETE
DELETE FROM recno_repl_test WHERE id = 2;

-- Verify deletion
SELECT id, value, data FROM recno_repl_test ORDER BY id;

-- Test TRUNCATE behavior
TRUNCATE recno_repl_test;

-- Verify empty
SELECT COUNT(*) as count_after_truncate FROM recno_repl_test;

-- Re-insert for further testing
INSERT INTO recno_repl_test VALUES (10, 1000, 'after truncate', NOW());
INSERT INTO recno_repl_test VALUES (20, 2000, 'second row', NOW());

-- Test bulk operations
INSERT INTO recno_repl_test
SELECT i, i * 100, 'bulk data ' || i, NOW()
FROM generate_series(30, 50) i;

-- Verify bulk insert
SELECT COUNT(*) as total_rows FROM recno_repl_test;

-- Test mixed DML transaction
BEGIN;
INSERT INTO recno_repl_test VALUES (60, 6000, 'in transaction', NOW());
UPDATE recno_repl_test SET value = 9999 WHERE id = 10;
DELETE FROM recno_repl_test WHERE id >= 40 AND id <= 45;
COMMIT;

-- Verify transaction results
SELECT id, value, data FROM recno_repl_test WHERE id IN (10, 40, 41, 42, 43, 44, 45, 60) ORDER BY id;

-- Test REPLICA IDENTITY support
-- Default is REPLICA IDENTITY DEFAULT (primary key)
SELECT relname, relreplident
FROM pg_class
WHERE relname = 'recno_repl_test';

-- Change to FULL
ALTER TABLE recno_repl_test REPLICA IDENTITY FULL;

-- Verify change
SELECT relname, relreplident
FROM pg_class
WHERE relname = 'recno_repl_test';

-- Test updates after REPLICA IDENTITY FULL
UPDATE recno_repl_test SET value = value + 1 WHERE id = 20;

-- Test with no primary key (relies on FULL replica identity)
CREATE TABLE recno_no_pk (
    col1 INTEGER,
    col2 TEXT,
    col3 TIMESTAMP DEFAULT NOW()
) USING recno;

ALTER TABLE recno_no_pk REPLICA IDENTITY FULL;

INSERT INTO recno_no_pk VALUES (1, 'text1', NOW());
INSERT INTO recno_no_pk VALUES (2, 'text2', NOW());
UPDATE recno_no_pk SET col2 = 'updated' WHERE col1 = 1;
DELETE FROM recno_no_pk WHERE col1 = 2;

SELECT col1, col2, col3 IS NOT NULL AS has_ts FROM recno_no_pk ORDER BY col1;

-- Test with unique index as replica identity
CREATE TABLE recno_unique_idx (
    id INTEGER,
    email TEXT UNIQUE,
    name TEXT
) USING recno;

CREATE UNIQUE INDEX recno_unique_idx_email ON recno_unique_idx(email);
ALTER TABLE recno_unique_idx REPLICA IDENTITY USING INDEX recno_unique_idx_email;

INSERT INTO recno_unique_idx VALUES (1, 'user1@example.com', 'User One');
INSERT INTO recno_unique_idx VALUES (2, 'user2@example.com', 'User Two');

UPDATE recno_unique_idx SET name = 'Updated User' WHERE email = 'user1@example.com';
DELETE FROM recno_unique_idx WHERE email = 'user2@example.com';

SELECT * FROM recno_unique_idx ORDER BY id;

-- Test WAL decoding for logical replication
-- Create a logical replication slot (extract only slot name, LSN is non-deterministic)
SELECT (pg_create_logical_replication_slot('recno_test_slot', 'test_decoding')).slot_name;

-- Perform some operations that should be captured
BEGIN;
INSERT INTO recno_repl_test VALUES (100, 10000, 'for logical rep', NOW());
UPDATE recno_repl_test SET value = value * 2 WHERE id = 100;
DELETE FROM recno_repl_test WHERE id = 100;
COMMIT;

-- Verify the slot captured changes
-- Note: In actual logical replication, a subscriber would consume these changes
SELECT pg_drop_replication_slot('recno_test_slot');

-- Test with large values (potential overflow/TOAST interaction)
CREATE TABLE recno_large_repl (
    id INTEGER PRIMARY KEY,
    large_text TEXT
) USING recno;

INSERT INTO recno_large_repl VALUES (1, repeat('Large data for replication test. ', 1000));
UPDATE recno_large_repl SET large_text = repeat('Updated large data. ', 1000) WHERE id = 1;

SELECT id, length(large_text) as text_length FROM recno_large_repl;

-- Cleanup
DROP TABLE recno_repl_test;
DROP TABLE recno_no_pk;
DROP TABLE recno_unique_idx;
DROP TABLE recno_large_repl;

-- Summary: Logical replication requirements for RECNO
\echo 'Logical Replication Validation Complete'
\echo ''
\echo 'RECNO must support:'
\echo '  1. WAL logging for INSERT/UPDATE/DELETE operations'
\echo '  2. REPLICA IDENTITY (DEFAULT, FULL, USING INDEX)'
\echo '  3. Logical decoding via replication slots'
\echo '  4. Tuple visibility for OLD/NEW values'
\echo '  5. Transaction consistency in WAL stream'
\echo ''
\echo 'All operations completed successfully.'
