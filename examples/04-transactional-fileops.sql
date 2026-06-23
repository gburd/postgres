-- ============================================================================
-- Example 4: Transactional File Operations (FILEOPS)
-- ============================================================================
-- Demonstrates WAL-logged, transactional table creation and deletion.
-- FILEOPS is always-on infrastructure; no GUC configuration is needed.

-- Example 1: Table creation survives crashes
BEGIN;

CREATE TABLE crash_safe_data (
    id   serial PRIMARY KEY,
    data text
);

-- At this point, a XLOG_FILEOPS_CREATE WAL record has been written.
-- If the server crashes before COMMIT, the file will be automatically deleted.

INSERT INTO crash_safe_data (data) VALUES ('test data');

COMMIT;

-- The file is now durable; CREATE and data are atomic.

-- Example 2: Table deletion is deferred until commit
BEGIN;

DROP TABLE crash_safe_data;

-- The relation file still exists on disk (deletion deferred).
-- A XLOG_FILEOPS_DELETE WAL record has been written.

COMMIT;

-- Now the file is deleted atomically with the transaction commit.

-- Example 3: Rollback properly cleans up created files
BEGIN;

CREATE TABLE temp_table (id int);
INSERT INTO temp_table VALUES (1), (2), (3);

-- File exists on disk with data.

ROLLBACK;

-- File is automatically deleted (FILEOPS cleanup on abort).
-- No orphaned files left behind.
