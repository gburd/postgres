-- ============================================================================
-- Example 5: Monitoring UNDO Subsystem
-- ============================================================================

-- View UNDO log statistics
SELECT
    log_number,
    insert_ptr,
    discard_ptr,
    used_bytes,
    active_xacts,
    last_discard_time
FROM pg_stat_undo_logs
ORDER BY log_number;

-- View UNDO buffer statistics
SELECT
    buffer_hits,
    buffer_misses,
    buffer_evictions,
    hit_ratio
FROM pg_stat_undo_buffers;

-- Check UNDO directory size
SELECT pg_size_pretty(
    pg_total_relation_size('base/undo')
) AS undo_dir_size;

-- List tables with UNDO enabled
SELECT
    n.nspname AS schema,
    c.relname AS table,
    c.reloptions
FROM pg_class c
JOIN pg_namespace n ON c.relnamespace = n.oid
WHERE c.reloptions::text LIKE '%enable_undo=on%'
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
