-- ============================================================================
-- Example 5: Monitoring UNDO Subsystem
-- ============================================================================

-- View UNDO log statistics
SELECT * FROM pg_stat_get_undo_logs();

-- View UNDO buffer statistics
SELECT * FROM pg_stat_get_undo_buffers();

-- Force discard of UNDO records older than the retention horizon
-- (normally handled automatically by the UNDO worker)
SELECT pg_undo_force_discard();

-- List tables using an AM that supports UNDO (i.e., flux tables)
SELECT
    n.nspname AS schema,
    c.relname AS table,
    am.amname AS access_method
FROM pg_class c
JOIN pg_namespace n ON c.relnamespace = n.oid
JOIN pg_am am ON c.relam = am.oid
WHERE am.amname = 'flux'
ORDER BY n.nspname, c.relname;

-- Monitor UNDO worker activity
SELECT
    pid,
    backend_type,
    state,
    query_start,
    state_change
FROM pg_stat_activity
WHERE backend_type = 'undo worker';

-- Check current UNDO retention settings
SHOW undo_retention_time;
SHOW undo_worker_naptime;
