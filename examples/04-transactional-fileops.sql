--
-- Example: Transactional file operations (FILEOPS)
--
-- This example demonstrates WAL-logged file system operations that
-- integrate with PostgreSQL's transaction system.
--

-- FILEOPS provides atomic guarantees for:
-- - Creating/dropping relation forks
-- - Extending relation forks
-- - File operations with crash recovery

-- Note: This is a low-level infrastructure feature.
-- Most users will not interact with FILEOPS directly.
-- It is used internally by per-relation UNDO and can be used
-- by custom table access methods or extensions.

-- Example: Table AM using FILEOPS to create custom fork
-- (This is illustrative - actual usage is via C API)

-- When a table AM creates a per-relation UNDO fork:
--   1. FileOpsCreate(rel, RELUNDO_FORKNUM)  -- Create fork
--   2. FileOpsExtend(rel, RELUNDO_FORKNUM, 10)  -- Extend by 10 blocks
--   3. On COMMIT: Changes are permanent
--   4. On ROLLBACK: Fork creation is reversed

-- The key benefit: File operations participate in transactions
-- Without FILEOPS: File created, transaction aborts, orphan file remains
-- With FILEOPS: File created, transaction aborts, file automatically removed

-- FILEOPS operations are WAL-logged:
-- - Crash during CREATE: Redo creates the file
-- - Crash after ROLLBACK: Undo removes the file
-- - Standby replay: File operations are replayed correctly

-- GUC configuration:
-- enable_transactional_fileops = on  (default)

-- For extension developers:
-- See src/include/storage/fileops.h for C API documentation
-- See src/backend/access/undo/relundo.c for usage examples
