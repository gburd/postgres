--
-- Example: Per-Relation UNDO using test_undo_tam
--
-- This example demonstrates per-relation UNDO, which stores operation
-- metadata in each table's UNDO fork for MVCC visibility and rollback.
--

-- Load the test table access method
CREATE EXTENSION IF NOT EXISTS test_undo_tam;

-- Create a table using the test AM (which uses per-relation UNDO)
CREATE TABLE demo_relundo (
    id int,
    data text
) USING test_undo_tam;

-- Insert some data
-- Each INSERT creates an UNDO record in the table's UNDO fork
INSERT INTO demo_relundo VALUES (1, 'first row');
INSERT INTO demo_relundo VALUES (2, 'second row');
INSERT INTO demo_relundo VALUES (3, 'third row');

-- Query the data
SELECT * FROM demo_relundo ORDER BY id;

-- Inspect the UNDO chain (test_undo_tam provides introspection)
SELECT undo_ptr, rec_type, xid, first_tid, end_tid
FROM test_undo_tam_dump_chain('demo_relundo'::regclass)
ORDER BY undo_ptr DESC;

-- Rollback demonstration
BEGIN;
INSERT INTO demo_relundo VALUES (4, 'will be rolled back');
SELECT * FROM demo_relundo ORDER BY id;  -- Shows 4 rows

-- Process pending async UNDO work (for test determinism)
SELECT test_undo_tam_process_pending();
ROLLBACK;

-- After rollback, row 4 is gone (async worker applied UNDO)
SELECT test_undo_tam_process_pending();  -- Drain worker queue
SELECT * FROM demo_relundo ORDER BY id;  -- Shows 3 rows

-- UNDO chain after rollback
SELECT undo_ptr, rec_type, xid, first_tid, end_tid
FROM test_undo_tam_dump_chain('demo_relundo'::regclass)
ORDER BY undo_ptr DESC;

-- Cleanup
DROP TABLE demo_relundo;

--
-- Architecture notes:
--
-- Per-relation UNDO differs from cluster-wide UNDO:
--
-- Cluster-wide UNDO (heap with enable_undo=on):
--   - Stores complete tuple data in global UNDO logs (base/undo/)
--   - Synchronous rollback via UndoReplay()
--   - Shared across all tables using UNDO
--   - Space managed globally
--
-- Per-relation UNDO (custom table AMs):
--   - Stores metadata in table's UNDO fork (relfilenode.undo)
--   - Async rollback via background workers
--   - Independent per-table management
--   - Space managed per-relation
--
-- When to use per-relation UNDO:
--   - Custom table AMs needing MVCC without heap overhead
--   - Columnar storage (delta UNDO records)
--   - Workloads benefiting from per-table UNDO isolation
--
-- When to use cluster-wide UNDO:
--   - Standard heap tables
--   - Workloads with frequent aborts
--   - Need for fast synchronous rollback
--
