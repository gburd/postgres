--
-- Test NULL handling optimizations (NO_NULLS, SPARSE_NULLS, RLE_NULLS)
-- Verifies that NULL bitmap is omitted or optimized based on NULL density.
--

-- Test 1: NO_NULLS optimization (column has zero NULLs)
CREATE TABLE orvos_no_nulls_test (
    id int NOT NULL,
    value text NOT NULL,
    amount int NOT NULL
) USING orvos;

INSERT INTO orvos_no_nulls_test
SELECT i, 'value_' || i, i * 10
FROM generate_series(1, 100) i;

SELECT COUNT(*) FROM orvos_no_nulls_test;
SELECT * FROM orvos_no_nulls_test WHERE id <= 5 ORDER BY id;

DROP TABLE orvos_no_nulls_test;

-- Test 2: SPARSE_NULLS optimization (<5% NULL density)
CREATE TABLE orvos_sparse_nulls_test (
    id int,
    value text,
    amount int
) USING orvos;

-- Insert 95 non-NULL rows and 5 NULL rows
INSERT INTO orvos_sparse_nulls_test
SELECT i, 'value_' || i, i * 10
FROM generate_series(1, 95) i;

INSERT INTO orvos_sparse_nulls_test VALUES
    (96, NULL, 960),
    (97, 'value_97', NULL),
    (98, NULL, NULL),
    (99, 'value_99', 990),
    (100, NULL, 1000);

SELECT COUNT(*) FROM orvos_sparse_nulls_test WHERE value IS NULL;
SELECT COUNT(*) FROM orvos_sparse_nulls_test WHERE amount IS NULL;
SELECT * FROM orvos_sparse_nulls_test WHERE value IS NULL ORDER BY id;

DROP TABLE orvos_sparse_nulls_test;

-- Test 3: RLE_NULLS optimization (sequential NULLs)
CREATE TABLE orvos_rle_nulls_test (
    id int,
    value text
) USING orvos;

-- Insert pattern: 10 values, 20 NULLs, 10 values, 30 NULLs
INSERT INTO orvos_rle_nulls_test
SELECT i, 'value_' || i
FROM generate_series(1, 10) i;

INSERT INTO orvos_rle_nulls_test
SELECT i, NULL
FROM generate_series(11, 30) i;

INSERT INTO orvos_rle_nulls_test
SELECT i, 'value_' || i
FROM generate_series(31, 40) i;

INSERT INTO orvos_rle_nulls_test
SELECT i, NULL
FROM generate_series(41, 70) i;

SELECT COUNT(*) FROM orvos_rle_nulls_test WHERE value IS NULL;
SELECT COUNT(*) FROM orvos_rle_nulls_test WHERE value IS NOT NULL;
SELECT * FROM orvos_rle_nulls_test WHERE id IN (9, 10, 11, 12, 29, 30, 31, 32) ORDER BY id;

DROP TABLE orvos_rle_nulls_test;

-- Test 4: High NULL density (50%+)
CREATE TABLE orvos_high_nulls_test (
    id int,
    value text
) USING orvos;

-- Insert alternating NULL and non-NULL
INSERT INTO orvos_high_nulls_test
SELECT i,
       CASE WHEN i % 2 = 0 THEN 'value_' || i ELSE NULL END
FROM generate_series(1, 100) i;

SELECT COUNT(*) FROM orvos_high_nulls_test WHERE value IS NULL;
SELECT COUNT(*) FROM orvos_high_nulls_test WHERE value IS NOT NULL;

DROP TABLE orvos_high_nulls_test;
