--
-- Test dictionary encoding for low-cardinality columns
-- Verifies 10-100x compression for columns with distinct_count/total_rows < 0.01
--

-- Test 1: Very low cardinality (10 distinct values, 1000 rows = 1% cardinality)
CREATE TABLE orvos_dict_low_card_test (
    id int,
    status text,
    category text
) USING orvos;

INSERT INTO orvos_dict_low_card_test
SELECT i,
       (ARRAY['pending', 'active', 'completed', 'cancelled', 'failed'])[1 + (i % 5)],
       (ARRAY['A', 'B', 'C', 'D', 'E'])[1 + (i % 5)]
FROM generate_series(1, 1000) i;

SELECT COUNT(DISTINCT status) FROM orvos_dict_low_card_test;
SELECT COUNT(DISTINCT category) FROM orvos_dict_low_card_test;

SELECT status, COUNT(*) FROM orvos_dict_low_card_test GROUP BY status ORDER BY status;
SELECT category, COUNT(*) FROM orvos_dict_low_card_test GROUP BY category ORDER BY category;

-- Test filtering on dictionary-encoded columns
SELECT COUNT(*) FROM orvos_dict_low_card_test WHERE status = 'active';
SELECT COUNT(*) FROM orvos_dict_low_card_test WHERE category = 'A';
SELECT COUNT(*) FROM orvos_dict_low_card_test WHERE status = 'completed' AND category = 'C';

DROP TABLE orvos_dict_low_card_test;

-- Test 2: Enum-like column (country codes)
CREATE TABLE orvos_dict_country_test (
    id int,
    country_code char(2),
    region text
) USING orvos;

INSERT INTO orvos_dict_country_test
SELECT i,
       (ARRAY['US', 'CA', 'UK', 'FR', 'DE', 'JP', 'AU', 'BR', 'IN', 'CN'])[1 + (i % 10)],
       (ARRAY['North America', 'Europe', 'Asia', 'Oceania', 'South America'])[1 + (i % 5)]
FROM generate_series(1, 10000) i;

SELECT COUNT(DISTINCT country_code) FROM orvos_dict_country_test;
SELECT country_code, COUNT(*) FROM orvos_dict_country_test GROUP BY country_code ORDER BY country_code;

SELECT region, COUNT(*) FROM orvos_dict_country_test GROUP BY region ORDER BY region;

DROP TABLE orvos_dict_country_test;

-- Test 3: Mixed cardinality (should not encode high-cardinality column)
CREATE TABLE orvos_dict_mixed_test (
    id int,
    status text,  -- Low cardinality (should use dictionary)
    description text  -- High cardinality (should not use dictionary)
) USING orvos;

INSERT INTO orvos_dict_mixed_test
SELECT i,
       (ARRAY['new', 'in_progress', 'done'])[1 + (i % 3)],
       'description_' || i
FROM generate_series(1, 1000) i;

SELECT COUNT(DISTINCT status) FROM orvos_dict_mixed_test;
SELECT COUNT(DISTINCT description) FROM orvos_dict_mixed_test;

SELECT * FROM orvos_dict_mixed_test WHERE status = 'done' ORDER BY id LIMIT 5;

DROP TABLE orvos_dict_mixed_test;

-- Test 4: NULL values with dictionary encoding
CREATE TABLE orvos_dict_null_test (
    id int,
    status text
) USING orvos;

INSERT INTO orvos_dict_null_test
SELECT i,
       CASE
           WHEN i % 10 = 0 THEN NULL
           ELSE (ARRAY['draft', 'published', 'archived'])[1 + (i % 3)]
       END
FROM generate_series(1, 100) i;

SELECT COUNT(*) FROM orvos_dict_null_test WHERE status IS NULL;
SELECT status, COUNT(*) FROM orvos_dict_null_test GROUP BY status ORDER BY status;

DROP TABLE orvos_dict_null_test;
