-- ============================================================================
-- Example 1: Basic UNDO Setup and Tuple Recovery
-- ============================================================================
-- This example demonstrates:
-- 1. Enabling the UNDO subsystem at server level
-- 2. Creating an UNDO-enabled table
-- 3. Performing modifications
-- 4. Recovering pruned data with pg_undorecover

-- STEP 1: Enable UNDO at server level (requires restart)
-- Edit postgresql.conf:
--   enable_undo = on
-- Then: pg_ctl restart

-- STEP 2: Create an UNDO-enabled table
CREATE TABLE customer_data (
    id          serial PRIMARY KEY,
    name        text NOT NULL,
    email       text,
    created_at  timestamptz DEFAULT now()
) WITH (enable_undo = on);

-- STEP 3: Insert sample data
INSERT INTO customer_data (name, email) VALUES
    ('Alice Smith', 'alice@example.com'),
    ('Bob Johnson', 'bob@example.com'),
    ('Charlie Brown', 'charlie@example.com');

-- STEP 4: Perform an update
UPDATE customer_data SET email = 'alice.smith@newdomain.com' WHERE name = 'Alice Smith';

-- STEP 5: Accidentally delete data
DELETE FROM customer_data WHERE id = 2;

-- STEP 6: Commit the transaction
COMMIT;

-- STEP 7: Later, realize you need the deleted data
-- If the data has been pruned by HOT or VACUUM, use pg_undorecover:
-- $ pg_undorecover --relation=customer_data --oid=16384

-- STEP 8: Verify UNDO logs are being created
SELECT pg_ls_dir('base/undo');

-- STEP 9: Check UNDO statistics
SELECT * FROM pg_stat_undo_logs;
SELECT * FROM pg_stat_undo_buffers;
