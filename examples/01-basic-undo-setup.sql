-- ============================================================================
-- Example 1: Basic UNDO Setup and Monitoring
-- ============================================================================
-- This example demonstrates:
-- 1. Creating a table that uses UNDO (via the flux access method)
-- 2. Performing modifications
-- 3. Monitoring UNDO activity

-- STEP 1: Create a table using the flux AM (which supports UNDO)
-- No server-level configuration is needed; UNDO is always-on infrastructure.
CREATE TABLE customer_data (
    id          serial PRIMARY KEY,
    name        text NOT NULL,
    email       text,
    created_at  timestamptz DEFAULT now()
) USING flux;

-- STEP 2: Insert sample data
INSERT INTO customer_data (name, email) VALUES
    ('Alice Smith', 'alice@example.com'),
    ('Bob Johnson', 'bob@example.com'),
    ('Charlie Brown', 'charlie@example.com');

-- STEP 3: Perform an update (in-place for flux)
UPDATE customer_data SET email = 'alice.smith@newdomain.com' WHERE name = 'Alice Smith';

-- STEP 4: Delete a row
DELETE FROM customer_data WHERE id = 2;

-- STEP 5: Commit the transaction
COMMIT;

-- STEP 6: Check UNDO log statistics
SELECT * FROM pg_stat_get_undo_logs();

-- STEP 7: Check UNDO buffer statistics
SELECT * FROM pg_stat_get_undo_buffers();

-- STEP 8: Verify the UNDO worker is running
SELECT pid, backend_type, state
FROM pg_stat_activity
WHERE backend_type = 'undo worker';
