-- Enable tracking and detailed logging
SET enable_heap_prune_tracking = on;
SET log_min_messages = debug2;

-- Disable autovacuum globally
ALTER SYSTEM SET autovacuum = off;
SELECT pg_reload_conf();

CREATE EXTENSION IF NOT EXISTS pageinspect;

-- Drop existing functions
DROP FUNCTION IF EXISTS capture_prune_stats();
DROP FUNCTION IF EXISTS analyze_exit_reasons();

-- Updated function to capture statistics with exit reasons
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

SELECT pg_stat_reset();

\echo '=== PHASE 1: Setup with Massive Tuples ==='

CREATE TABLE ultimate_prune_test (
    id serial PRIMARY KEY,
    data text,
    status varchar(20) DEFAULT 'active',
    counter bigint DEFAULT 0,
    -- Make tuples absolutely massive - ~6KB each
    mega_padding1 text DEFAULT repeat('AAAAAAAA', 750),  -- 6KB
    mega_padding2 text DEFAULT repeat('BBBBBBBB', 750),  -- 6KB
    mega_padding3 text DEFAULT repeat('CCCCCCCC', 750)   -- 6KB
);

ALTER TABLE ultimate_prune_test SET (autovacuum_enabled = false);
CREATE INDEX idx_ultimate_status ON ultimate_prune_test(status);

-- Insert fewer rows but much larger
INSERT INTO ultimate_prune_test (data, mega_padding1, mega_padding2, mega_padding3)
SELECT
    'initial_' || i,
    repeat('INIT1_' || i, 750),
    repeat('INIT2_' || i, 750),
    repeat('INIT3_' || i, 750)
FROM generate_series(1, 20) i;  -- Only 20 rows but each is ~18KB

COMMIT;

SELECT 'BASELINE' as phase, * FROM capture_prune_stats();

\echo '=== PHASE 2: Create Dead Tuples in Separate Transactions ==='

-- Transaction 1: Create dead tuples
BEGIN;
UPDATE ultimate_prune_test SET
    data = 'dead_v1_' || id,
    counter = counter + 1000,
    mega_padding1 = repeat('DEAD1_V1_' || id, 750),
    mega_padding2 = repeat('DEAD2_V1_' || id, 750),
    mega_padding3 = repeat('DEAD3_V1_' || id, 750)
WHERE id BETWEEN 1 AND 10;
COMMIT;

\echo '--- After First Dead Tuple Creation ---'
SELECT * FROM capture_prune_stats() WHERE calls_total > 0;
SELECT * FROM analyze_exit_reasons();

-- Transaction 2: More dead tuples
BEGIN;
UPDATE ultimate_prune_test SET
    data = 'dead_v2_' || id,
    counter = counter + 2000,
    mega_padding1 = repeat('DEAD1_V2_' || id, 750),
    mega_padding2 = repeat('DEAD2_V2_' || id, 750),
    mega_padding3 = repeat('DEAD3_V2_' || id, 750)
WHERE id BETWEEN 11 AND 20;
COMMIT;

\echo '--- After Second Dead Tuple Creation ---'
SELECT * FROM capture_prune_stats() WHERE calls_total > 0;
SELECT * FROM analyze_exit_reasons();

-- Force visibility with VACUUM
VACUUM (ANALYZE, VERBOSE) ultimate_prune_test;

\echo '--- After VACUUM ---'
SELECT * FROM capture_prune_stats() WHERE calls_total > 0;
SELECT * FROM analyze_exit_reasons();

\echo '=== PHASE 3: Extreme INSERT Test ==='

CREATE TABLE insert_mega_test (
    id serial,
    huge_data text DEFAULT repeat('XXXXXXXX', 1500)  -- 12KB per tuple
);

ALTER TABLE insert_mega_test SET (autovacuum_enabled = false);

-- Insert only 1 tuple to nearly fill a page
INSERT INTO insert_mega_test (huge_data)
VALUES (repeat('HUGE_INITIAL', 1500));

COMMIT;

-- Create dead tuple
BEGIN;
UPDATE insert_mega_test SET huge_data = repeat('DEAD_HUGE', 1500);
COMMIT;

-- Force visibility
VACUUM (ANALYZE, VERBOSE) insert_mega_test;

-- Now insert - should trigger INSERT_SPACE_CHECK
INSERT INTO insert_mega_test (huge_data)
VALUES (repeat('TRIGGER_INSERT_PRUNING', 1500));

\echo '--- After INSERT Test ---'
SELECT * FROM capture_prune_stats() WHERE calls_total > 0;
SELECT * FROM analyze_exit_reasons();

DROP TABLE insert_mega_test;

\echo '=== PHASE 4: UPDATE Test with Extreme Sizes ==='

-- Create more dead tuples
BEGIN;
UPDATE ultimate_prune_test SET
    data = 'more_dead_' || id,
    counter = counter + 10000,
    mega_padding1 = repeat('MOREDEAD1_' || id, 800),
    mega_padding2 = repeat('MOREDEAD2_' || id, 800),
    mega_padding3 = repeat('MOREDEAD3_' || id, 800)
WHERE id BETWEEN 1 AND 10;
COMMIT;

-- Force visibility
VACUUM (ANALYZE, VERBOSE) ultimate_prune_test;

-- Large updates that should trigger UPDATE_FULL_PAGE
BEGIN;
UPDATE ultimate_prune_test SET
    data = 'GIGANTIC_UPDATE_' || repeat('X', 500) || '_' || id,
    counter = counter + 100000,
    mega_padding1 = repeat('GIGANTIC1_' || id, 1000),
    mega_padding2 = repeat('GIGANTIC2_' || id, 1000),
    mega_padding3 = repeat('GIGANTIC3_' || id, 1000)
WHERE id BETWEEN 1 AND 5;
COMMIT;

\echo '--- After UPDATE Test ---'
SELECT * FROM capture_prune_stats() WHERE calls_total > 0;
SELECT * FROM analyze_exit_reasons();

\echo '=== PHASE 5: Scan Pressure Test ==='

-- Create more dead tuples for scanning
BEGIN;
UPDATE ultimate_prune_test SET
    data = 'scan_dead_' || id,
    counter = counter + 1000000,
    mega_padding1 = repeat('SCANDEAD1_' || id, 900),
    mega_padding2 = repeat('SCANDEAD2_' || id, 900),
    mega_padding3 = repeat('SCANDEAD3_' || id, 900)
WHERE id BETWEEN 11 AND 20;
COMMIT;

-- Force visibility
VACUUM (ANALYZE, VERBOSE) ultimate_prune_test;

-- Intensive scanning to trigger SCAN_OPPORTUNISTIC
SELECT count(*) FROM ultimate_prune_test WHERE data LIKE 'scan_dead_%';
SELECT count(*) FROM ultimate_prune_test WHERE counter > 500000;
SELECT avg(length(mega_padding1)) FROM ultimate_prune_test;

-- Force sequential scans
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT count(*) FROM ultimate_prune_test WHERE status = 'active';
SELECT count(*) FROM ultimate_prune_test WHERE length(data) > 50;
RESET enable_indexscan;
RESET enable_bitmapscan;

\echo '--- After SCAN Test ---'
SELECT * FROM capture_prune_stats() WHERE calls_total > 0;
SELECT * FROM analyze_exit_reasons();

\echo '=== PHASE 6: Multi-Insert Test ==='

-- Bulk insert with massive tuples
INSERT INTO ultimate_prune_test (data, mega_padding1, mega_padding2, mega_padding3)
SELECT
    'bulk_mega_' || i,
    repeat('BULKMEGA1_' || i, 900),
    repeat('BULKMEGA2_' || i, 900),
    repeat('BULKMEGA3_' || i, 900)
FROM generate_series(21, 40) i;

\echo '--- After MULTI_INSERT Test ---'
SELECT * FROM capture_prune_stats() WHERE calls_total > 0;
SELECT * FROM analyze_exit_reasons();

\echo '=== PHASE 7: Ultimate Stress Test ==='

-- Create extreme conditions without DO blocks
-- Round 1: Massive updates
BEGIN;
UPDATE ultimate_prune_test SET
    data = 'stress1_' || id,
    counter = counter + 10000000,
    mega_padding1 = repeat('STRESS1A_' || id, 1200),
    mega_padding2 = repeat('STRESS1B_' || id, 1200),
    mega_padding3 = repeat('STRESS1C_' || id, 1200)
WHERE id BETWEEN 1 AND 20;
COMMIT;

-- Round 2: More massive updates
BEGIN;
UPDATE ultimate_prune_test SET
    data = 'stress2_' || id,
    counter = counter + 20000000,
    mega_padding1 = repeat('STRESS2A_' || id, 1200),
    mega_padding2 = repeat('STRESS2B_' || id, 1200),
    mega_padding3 = repeat('STRESS2C_' || id, 1200)
WHERE id BETWEEN 21 AND 40;
COMMIT;

-- Round 3: Inserts
INSERT INTO ultimate_prune_test (data, mega_padding1, mega_padding2, mega_padding3)
SELECT
    'stress_insert_' || i,
    repeat('STRESSINS1_' || i, 1200),
    repeat('STRESSINS2_' || i, 1200),
    repeat('STRESSINS3_' || i, 1200)
FROM generate_series(41, 60) i;

\echo '--- After STRESS Test ---'
SELECT * FROM capture_prune_stats() WHERE calls_total > 0;
SELECT * FROM analyze_exit_reasons();

\echo '=== PHASE 8: Force Extreme Conditions ==='

-- Create table guaranteed to need pruning
CREATE TABLE force_pruning_test (
    id serial,
    data text DEFAULT repeat('Z', 8000)  -- 8KB per tuple, 1 per page
);

ALTER TABLE force_pruning_test SET (autovacuum_enabled = false);

-- Insert exactly 1 tuple (should fill most of a page)
INSERT INTO force_pruning_test (data) VALUES (repeat('FIRST', 2000));
COMMIT;

-- Make it dead
BEGIN;
UPDATE force_pruning_test SET data = repeat('DEAD_FIRST', 2000) WHERE id = 1;
COMMIT;

-- Force visibility
VACUUM (ANALYZE, VERBOSE) force_pruning_test;

-- This insert MUST trigger pruning
INSERT INTO force_pruning_test (data) VALUES (repeat('MUST_TRIGGER_PRUNING', 2000));

\echo '--- After FORCE PRUNING Test ---'
SELECT * FROM capture_prune_stats() WHERE calls_total > 0;
SELECT * FROM analyze_exit_reasons();

DROP TABLE force_pruning_test;

\echo '=== PHASE 9: Final Comprehensive Analysis ==='

-- Final statistics with detailed exit reason analysis
SELECT
    context,
    calls_total,
    pages_pruned,
    tuples_pruned,
    space_freed,
    exit_success,
    exit_invalid_xact_xid,
    exit_no_removable_xids,
    exit_page_not_prunable,
    exit_lock_failed,
    exit_other,
    prune_success_rate_pct
FROM capture_prune_stats()
WHERE calls_total > 0
ORDER BY calls_total DESC;

-- Exit reason summary
SELECT
    'EXIT_REASON_SUMMARY' as report_type,
    context,
    main_failure_reason,
    failure_count,
    failure_pct,
    success_rate_pct
FROM analyze_exit_reasons()
ORDER BY calls_total DESC;

-- Recommendations based on failure patterns
WITH recommendations AS (
    SELECT
        context,
        main_failure_reason,
        CASE main_failure_reason
            WHEN 'INVALID_XACT_XID' THEN 'Check prune_xid setting and page header'
            WHEN 'NO_REMOVABLE_XIDS' THEN 'Need more transaction churn or longer waits for visibility'
            WHEN 'PAGE_NOT_PRUNABLE' THEN 'Check PageHasPrunable() logic and page conditions'
            WHEN 'LOCK_FAILED' THEN 'Increase lock acquisition attempts or use different strategy'
            ELSE 'Investigate other failure causes'
        END as recommendation
    FROM analyze_exit_reasons()
    WHERE calls_total > 0
)
SELECT
    'RECOMMENDATIONS' as report_type,
    context,
    main_failure_reason,
    recommendation
FROM recommendations;

-- Table statistics
SELECT
    schemaname, relname, n_tup_ins, n_tup_upd, n_tup_hot_upd, n_dead_tup,
    CASE WHEN n_tup_upd > 0 THEN round(100.0 * n_tup_hot_upd / n_tup_upd, 2) ELSE 0 END as hot_update_pct,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||relname)) as table_size,
    pg_size_pretty(pg_relation_size(schemaname||'.'||relname)) as heap_size
FROM pg_stat_user_tables
WHERE relname = 'ultimate_prune_test';

\echo '=== Test Complete ==='

ALTER SYSTEM SET autovacuum = on;
SELECT pg_reload_conf();

DROP TABLE ultimate_prune_test;
