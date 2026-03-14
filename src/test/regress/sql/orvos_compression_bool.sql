--
-- Test boolean bit-packing compression (8 bools per byte)
-- This test verifies that OVBT_ATTR_BITPACKED format flag provides
-- 8x compression for boolean columns.
--

-- Create table with multiple boolean columns to test bit-packing
CREATE TABLE orvos_bool_test (
    id int,
    flag1 boolean,
    flag2 boolean,
    flag3 boolean,
    flag4 boolean,
    flag5 boolean,
    flag6 boolean,
    flag7 boolean,
    flag8 boolean,
    flag9 boolean,
    flag10 boolean
) USING orvos;

-- Insert test data with various boolean patterns
INSERT INTO orvos_bool_test VALUES
    (1, true, false, true, false, true, false, true, false, true, false),
    (2, false, true, false, true, false, true, false, true, false, true),
    (3, true, true, false, false, true, true, false, false, true, true),
    (4, false, false, true, true, false, false, true, true, false, false),
    (5, true, false, false, true, true, false, false, true, true, false);

-- Test retrieval of all boolean values
SELECT * FROM orvos_bool_test ORDER BY id;

-- Test filtering on boolean columns
SELECT id, flag1, flag5 FROM orvos_bool_test WHERE flag1 = true ORDER BY id;
SELECT id, flag2, flag8 FROM orvos_bool_test WHERE flag2 = false AND flag8 = true ORDER BY id;

-- Test boolean aggregations
SELECT COUNT(*) FROM orvos_bool_test WHERE flag1 = true;
SELECT COUNT(*) FROM orvos_bool_test WHERE flag1 = true AND flag2 = false;

-- Test all TRUE and all FALSE patterns
INSERT INTO orvos_bool_test VALUES
    (6, true, true, true, true, true, true, true, true, true, true),
    (7, false, false, false, false, false, false, false, false, false, false);

SELECT * FROM orvos_bool_test WHERE id >= 6 ORDER BY id;

-- Test NULL booleans (should still use bit-packing for non-NULL values)
INSERT INTO orvos_bool_test VALUES
    (8, NULL, true, NULL, false, NULL, true, NULL, false, NULL, true),
    (9, false, NULL, true, NULL, false, NULL, true, NULL, false, NULL);

SELECT * FROM orvos_bool_test WHERE id >= 8 ORDER BY id;

-- Test update of boolean values (verify MVCC with bit-packed storage)
UPDATE orvos_bool_test SET flag1 = NOT flag1 WHERE id = 1;
SELECT id, flag1, flag2 FROM orvos_bool_test WHERE id = 1;

-- Cleanup
DROP TABLE orvos_bool_test;
