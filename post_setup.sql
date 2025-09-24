-- Enable tracking (in case it was reset)
SET enable_heap_prune_tracking = on;

-- Disable autovacuum on pgbench tables to preserve dead tuples for pruning tests
ALTER TABLE pgbench_accounts SET (autovacuum_enabled = false);
ALTER TABLE pgbench_branches SET (autovacuum_enabled = false);
ALTER TABLE pgbench_tellers SET (autovacuum_enabled = false);
ALTER TABLE pgbench_history SET (autovacuum_enabled = false);

-- Add columns that can be updated without affecting indexes (for HOT updates)
ALTER TABLE pgbench_accounts ADD COLUMN IF NOT EXISTS last_updated timestamp DEFAULT now();
ALTER TABLE pgbench_accounts ADD COLUMN IF NOT EXISTS update_count int DEFAULT 0;
ALTER TABLE pgbench_accounts ADD COLUMN IF NOT EXISTS notes text DEFAULT 'initial';

-- Make the notes column much larger to create page pressure
ALTER TABLE pgbench_accounts ALTER COLUMN notes SET DEFAULT repeat('initial_data', 100);

-- Create a partial index to allow more HOT updates
CREATE INDEX IF NOT EXISTS idx_accounts_high_balance ON pgbench_accounts(abalance) WHERE abalance > 1000;

-- Reset statistics after setup
SELECT pg_stat_reset();

-- Show initial state
SELECT 'POST_SETUP_BASELINE' as phase, * FROM capture_prune_stats();
