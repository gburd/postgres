--
-- zero_vacuum.sql
--   Test that HEAP UNDO + NBTREE UNDO eliminates dead tuples from
--   aborted transactions, providing a "zero-VACUUM" experience.
--
-- Requires: enable_undo = on (set via undo_regress.conf)
--

-- UNDO-enabled table with btree index (PK)
CREATE TABLE zv_test(id int PRIMARY KEY, data text) WITH (enable_undo = on);

-- Aborted single-row insert leaves no dead tuples
BEGIN;
INSERT INTO zv_test VALUES (1, 'aborted');
ABORT;

-- After UNDO rollback, no dead tuples should remain
SELECT n_dead_tup FROM pg_stat_user_tables WHERE relname = 'zv_test';

-- Committed data works normally
INSERT INTO zv_test VALUES (100, 'committed');
SELECT * FROM zv_test WHERE id = 100;

-- Aborted bulk insert leaves no dead tuples
BEGIN;
INSERT INTO zv_test SELECT g, 'bulk_' || g FROM generate_series(1, 100) g;
ABORT;

-- Force stats update
ANALYZE zv_test;
SELECT n_dead_tup FROM pg_stat_user_tables WHERE relname = 'zv_test';

-- Committed insert + delete works correctly with UNDO
DELETE FROM zv_test WHERE id = 100;
SELECT * FROM zv_test WHERE id = 100;

-- Verify index consistency
SELECT bt_index_check(indexrelid)
  FROM pg_index WHERE indrelid = 'zv_test'::regclass;

-- Cleanup
DROP TABLE zv_test;
