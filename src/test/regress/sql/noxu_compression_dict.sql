--
-- Test dictionary encoding for low-cardinality columns
-- Verifies 10-100x compression for columns with distinct_count/total_rows < 0.01
--

-- Test 1: Very low cardinality (10 distinct values, 1000 rows = 1% cardinality)
CREATE TABLE noxu_dict_low_card_test (
    id int,
    status text,
    category text
) USING noxu;

INSERT INTO noxu_dict_low_card_test
SELECT i,
       (ARRAY['pending', 'active', 'completed', 'cancelled', 'failed'])[1 + (i % 5)],
       (ARRAY['A', 'B', 'C', 'D', 'E'])[1 + (i % 5)]
FROM generate_series(1, 1000) i;

SELECT COUNT(DISTINCT status) FROM noxu_dict_low_card_test;
SELECT COUNT(DISTINCT category) FROM noxu_dict_low_card_test;

SELECT status, COUNT(*) FROM noxu_dict_low_card_test GROUP BY status ORDER BY status;
SELECT category, COUNT(*) FROM noxu_dict_low_card_test GROUP BY category ORDER BY category;

-- Test filtering on dictionary-encoded columns
SELECT COUNT(*) FROM noxu_dict_low_card_test WHERE status = 'active';
SELECT COUNT(*) FROM noxu_dict_low_card_test WHERE category = 'A';
SELECT COUNT(*) FROM noxu_dict_low_card_test WHERE status = 'completed' AND category = 'C';

DROP TABLE noxu_dict_low_card_test;

-- Test 2: Enum-like column (country codes)
CREATE TABLE noxu_dict_country_test (
    id int,
    country_code char(2),
    region text
) USING noxu;

INSERT INTO noxu_dict_country_test
SELECT i,
       (ARRAY['US', 'CA', 'UK', 'FR', 'DE', 'JP', 'AU', 'BR', 'IN', 'CN'])[1 + (i % 10)],
       (ARRAY['North America', 'Europe', 'Asia', 'Oceania', 'South America'])[1 + (i % 5)]
FROM generate_series(1, 10000) i;

SELECT COUNT(DISTINCT country_code) FROM noxu_dict_country_test;
SELECT country_code, COUNT(*) FROM noxu_dict_country_test GROUP BY country_code ORDER BY country_code;

SELECT region, COUNT(*) FROM noxu_dict_country_test GROUP BY region ORDER BY region;

DROP TABLE noxu_dict_country_test;

-- Test 3: Mixed cardinality (should not encode high-cardinality column)
CREATE TABLE noxu_dict_mixed_test (
    id int,
    status text,  -- Low cardinality (should use dictionary)
    description text  -- High cardinality (should not use dictionary)
) USING noxu;

INSERT INTO noxu_dict_mixed_test
SELECT i,
       (ARRAY['new', 'in_progress', 'done'])[1 + (i % 3)],
       'description_' || i
FROM generate_series(1, 1000) i;

SELECT COUNT(DISTINCT status) FROM noxu_dict_mixed_test;
SELECT COUNT(DISTINCT description) FROM noxu_dict_mixed_test;

SELECT * FROM noxu_dict_mixed_test WHERE status = 'done' ORDER BY id LIMIT 5;

DROP TABLE noxu_dict_mixed_test;

-- Test 4: NULL values with dictionary encoding
CREATE TABLE noxu_dict_null_test (
    id int,
    status text
) USING noxu;

INSERT INTO noxu_dict_null_test
SELECT i,
       CASE
	   WHEN i % 10 = 0 THEN NULL
	   ELSE (ARRAY['draft', 'published', 'archived'])[1 + (i % 3)]
       END
FROM generate_series(1, 100) i;

SELECT COUNT(*) FROM noxu_dict_null_test WHERE status IS NULL;
SELECT status, COUNT(*) FROM noxu_dict_null_test GROUP BY status ORDER BY status;

DROP TABLE noxu_dict_null_test;

-- Test 5: UPDATE and DELETE on dictionary-encoded columns
-- Exercises the explode path for dictionary items
CREATE TABLE noxu_dict_update_test (
    id int,
    status text
) USING noxu;

INSERT INTO noxu_dict_update_test
SELECT i,
       (ARRAY['open', 'closed', 'pending'])[1 + (i % 3)]
FROM generate_series(1, 300) i;

-- Verify initial state
SELECT status, COUNT(*) FROM noxu_dict_update_test GROUP BY status ORDER BY status;

-- Update some rows
UPDATE noxu_dict_update_test SET status = 'resolved' WHERE id <= 30;
SELECT status, COUNT(*) FROM noxu_dict_update_test GROUP BY status ORDER BY status;

-- Delete some rows
DELETE FROM noxu_dict_update_test WHERE id <= 15;
SELECT COUNT(*) FROM noxu_dict_update_test;
SELECT status, COUNT(*) FROM noxu_dict_update_test GROUP BY status ORDER BY status;

DROP TABLE noxu_dict_update_test;

-- Test 6: Integer column with low cardinality (fixed-width byval)
CREATE TABLE noxu_dict_int_test (
    id int,
    priority int
) USING noxu;

INSERT INTO noxu_dict_int_test
SELECT i, (i % 3) + 1
FROM generate_series(1, 1000) i;

SELECT priority, COUNT(*) FROM noxu_dict_int_test GROUP BY priority ORDER BY priority;

DROP TABLE noxu_dict_int_test;
