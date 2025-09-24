-- Enable tracking
SET enable_heap_prune_tracking = on;
SET log_min_messages = debug2;

-- Drop existing functions to avoid signature conflicts
DROP FUNCTION IF EXISTS capture_prune_stats();
DROP FUNCTION IF EXISTS analyze_exit_reasons();

-- Function to capture current statistics with exit reasons
CREATE OR REPLACE FUNCTION capture_prune_stats()
RETURNS TABLE(
    context text,
    calls_total bigint,
    pages_pruned bigint,
    tuples_pruned bigint,
    space_freed bigint,
    time_spent_us bigint,
    exit_success bigint,
    exit_invalid_xact_xid bigint,
    exit_no_removable_xids bigint,
    exit_page_not_prunable bigint,
    exit_lock_failed bigint,
    exit_other bigint,
    prune_success_rate_pct numeric,
    avg_time_per_call_us numeric
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        s.context,
        s.calls_total,
        s.pages_pruned,
        s.tuples_pruned,
        s.space_freed,
        s.time_spent_us,
        s.exit_success,
        s.exit_invalid_xact_xid,
        s.exit_no_removable_xids,
        s.exit_page_not_prunable,
        s.exit_lock_failed,
        s.exit_other,
        CASE WHEN s.calls_total > 0
             THEN round(100.0 * s.pages_pruned / s.calls_total, 2)
             ELSE 0
        END as prune_success_rate_pct,
        CASE WHEN s.calls_total > 0
             THEN round(s.time_spent_us::numeric / s.calls_total, 3)
             ELSE 0
        END as avg_time_per_call_us
    FROM pg_stat_get_heap_prune_stats() AS s(
        context text,
        calls_total bigint,
        pages_pruned bigint,
        tuples_pruned bigint,
        space_freed bigint,
        time_spent_us bigint,
        exit_success bigint,
        exit_invalid_xact_xid bigint,
        exit_no_removable_xids bigint,
        exit_page_not_prunable bigint,
        exit_lock_failed bigint,
        exit_other bigint
    )
    ORDER BY s.calls_total DESC;
END;
$$ LANGUAGE plpgsql;

-- Function to analyze exit reasons
CREATE OR REPLACE FUNCTION analyze_exit_reasons()
RETURNS TABLE(
    context text,
    calls_total bigint,
    success_rate_pct numeric,
    main_failure_reason text,
    failure_count bigint,
    failure_pct numeric
) AS $$
BEGIN
    RETURN QUERY
    WITH exit_analysis AS (
        SELECT
            s.context as ctx,
            s.calls_total as total_calls,
            s.exit_success,
            s.exit_invalid_xact_xid,
            s.exit_no_removable_xids,
            s.exit_page_not_prunable,
            s.exit_lock_failed,
            s.exit_other,
            CASE WHEN s.calls_total > 0
                 THEN round(100.0 * s.exit_success / s.calls_total, 2)
                 ELSE 0
            END as success_rate
        FROM capture_prune_stats() s
        WHERE s.calls_total > 0
    ),
    failure_reasons AS (
        SELECT
            ea.ctx,
            ea.total_calls,
            ea.success_rate,
            CASE
                WHEN ea.exit_invalid_xact_xid >= GREATEST(ea.exit_no_removable_xids, ea.exit_page_not_prunable, ea.exit_lock_failed, ea.exit_other)
                THEN 'INVALID_XACT_XID'
                WHEN ea.exit_no_removable_xids >= GREATEST(ea.exit_invalid_xact_xid, ea.exit_page_not_prunable, ea.exit_lock_failed, ea.exit_other)
                THEN 'NO_REMOVABLE_XIDS'
                WHEN ea.exit_page_not_prunable >= GREATEST(ea.exit_invalid_xact_xid, ea.exit_no_removable_xids, ea.exit_lock_failed, ea.exit_other)
                THEN 'PAGE_NOT_PRUNABLE'
                WHEN ea.exit_lock_failed >= GREATEST(ea.exit_invalid_xact_xid, ea.exit_no_removable_xids, ea.exit_page_not_prunable, ea.exit_other)
                THEN 'LOCK_FAILED'
                ELSE 'OTHER'
            END as main_reason,
            GREATEST(ea.exit_invalid_xact_xid, ea.exit_no_removable_xids, ea.exit_page_not_prunable, ea.exit_lock_failed, ea.exit_other) as max_failure_count
        FROM exit_analysis ea
    )
    SELECT
        fr.ctx,
        fr.total_calls,
        fr.success_rate,
        fr.main_reason,
        fr.max_failure_count,
        CASE WHEN fr.total_calls > 0
             THEN round(100.0 * fr.max_failure_count / fr.total_calls, 2)
             ELSE 0
        END as failure_percentage
    FROM failure_reasons fr
    ORDER BY fr.total_calls DESC;
END;
$$ LANGUAGE plpgsql;

-- Function to provide recommendations based on exit reasons
CREATE OR REPLACE FUNCTION get_pruning_recommendations()
RETURNS TABLE(
    context text,
    main_failure_reason text,
    recommendation text,
    failure_pct numeric
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        ar.context,
        ar.main_failure_reason,
        CASE ar.main_failure_reason
            WHEN 'INVALID_XACT_XID' THEN 'Check prune_xid setting and page header - may need transaction visibility'
            WHEN 'NO_REMOVABLE_XIDS' THEN 'Need more transaction churn or longer waits for visibility - try VACUUM or wait'
            WHEN 'PAGE_NOT_PRUNABLE' THEN 'Check PageHasPrunable() logic - page may not have dead tuples'
            WHEN 'LOCK_FAILED' THEN 'Increase lock acquisition attempts or reduce contention'
            ELSE 'Investigate other failure causes in logs'
        END as recommendation,
        ar.failure_pct
    FROM analyze_exit_reasons() ar
    WHERE ar.calls_total > 0;
END;
$$ LANGUAGE plpgsql;

-- Reset statistics
SELECT pg_stat_reset();
