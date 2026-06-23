-- ============================================================================
-- Example 2: Transaction Rollback with UNDO
-- ============================================================================
-- Demonstrates how UNDO records enable efficient transaction rollback

-- Create a table using the recno AM (supports UNDO)
CREATE TABLE order_items (
    order_id   int,
    item_id    int,
    quantity   int,
    price      numeric(10,2)
) USING recno;

-- Begin transaction
BEGIN;

-- Insert multiple rows
INSERT INTO order_items VALUES
    (1001, 1, 5, 29.99),
    (1001, 2, 3, 49.99),
    (1001, 3, 1, 199.99);

-- Perform updates
UPDATE order_items SET quantity = 10 WHERE item_id = 1;
UPDATE order_items SET price = 44.99 WHERE item_id = 2;

-- Delete a row
DELETE FROM order_items WHERE item_id = 3;

-- Check current state (before rollback)
SELECT * FROM order_items;
-- Should show: 2 rows (items 1 and 2, modified)

-- Rollback the transaction
-- UNDO records will be applied automatically:
-- - item 3 re-inserted
-- - item 2 price restored to 49.99
-- - item 1 quantity restored to 5
-- - all 3 original inserts deleted
ROLLBACK;

-- Verify all changes were rolled back
SELECT * FROM order_items;
-- Should show: 0 rows (everything rolled back via UNDO)
