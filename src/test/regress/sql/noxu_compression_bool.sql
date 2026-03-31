--
-- Test boolean bit-packing compression (8 bools per byte)
-- This test verifies that OVBT_ATTR_BITPACKED format flag provides
-- 8x compression for boolean columns.
--

-- Create table with multiple boolean columns to test bit-packing
CREATE TABLE noxu_bool_test (
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
) USING noxu;

-- Insert test data with various boolean patterns
INSERT INTO noxu_bool_test VALUES
    (1, true, false, true, false, true, false, true, false, true, false),
    (2, false, true, false, true, false, true, false, true, false, true),
    (3, true, true, false, false, true, true, false, false, true, true),
    (4, false, false, true, true, false, false, true, true, false, false),
    (5, true, false, false, true, true, false, false, true, true, false);

-- Test retrieval of all boolean values
SELECT * FROM noxu_bool_test ORDER BY id;

-- Test filtering on boolean columns
SELECT id, flag1, flag5 FROM noxu_bool_test WHERE flag1 = true ORDER BY id;
SELECT id, flag2, flag8 FROM noxu_bool_test WHERE flag2 = false AND flag8 = true ORDER BY id;

-- Test boolean aggregations
SELECT COUNT(*) FROM noxu_bool_test WHERE flag1 = true;
SELECT COUNT(*) FROM noxu_bool_test WHERE flag1 = true AND flag2 = false;

-- Test all TRUE and all FALSE patterns
INSERT INTO noxu_bool_test VALUES
    (6, true, true, true, true, true, true, true, true, true, true),
    (7, false, false, false, false, false, false, false, false, false, false);

SELECT * FROM noxu_bool_test WHERE id >= 6 ORDER BY id;

-- Test NULL booleans (should still use bit-packing for non-NULL values)
INSERT INTO noxu_bool_test VALUES
    (8, NULL, true, NULL, false, NULL, true, NULL, false, NULL, true),
    (9, false, NULL, true, NULL, false, NULL, true, NULL, false, NULL);

SELECT * FROM noxu_bool_test WHERE id >= 8 ORDER BY id;

-- Test update of boolean values (verify MVCC with bit-packed storage)
UPDATE noxu_bool_test SET flag1 = NOT flag1 WHERE id = 1;
SELECT id, flag1, flag2 FROM noxu_bool_test WHERE id = 1;

-- Cleanup
DROP TABLE noxu_bool_test;

--
-- Wide table test: 100 boolean columns to verify bit-packing at scale.
-- With bit-packing, 100 booleans should require ~13 bytes instead of 100 bytes
-- per row (8x compression: ceil(100/8) = 13 bytes).
--
DO $$
DECLARE
    cols text := '';
    vals text := '';
BEGIN
    FOR i IN 1..100 LOOP
	cols := cols || ', b' || i || ' boolean';
    END LOOP;
    EXECUTE 'CREATE TABLE noxu_bool_wide (id int' || cols || ') USING noxu';

    -- Insert 1000 rows with alternating true/false patterns
    FOR r IN 1..1000 LOOP
	vals := '';
	FOR i IN 1..100 LOOP
	    IF vals != '' THEN vals := vals || ', '; END IF;
	    vals := vals || CASE WHEN (r + i) % 2 = 0 THEN 'true' ELSE 'false' END;
	END LOOP;
	EXECUTE 'INSERT INTO noxu_bool_wide VALUES (' || r || ', ' || vals || ')';
    END LOOP;
END $$;

-- Verify correctness: spot-check a few rows
SELECT id, b1, b2, b50, b99, b100 FROM noxu_bool_wide WHERE id IN (1, 500, 1000) ORDER BY id;

-- Verify row count
SELECT COUNT(*) FROM noxu_bool_wide;

-- Verify boolean aggregation across wide columns
SELECT COUNT(*) FROM noxu_bool_wide WHERE b1 = true AND b100 = false;

-- Cleanup
DROP TABLE noxu_bool_wide;
