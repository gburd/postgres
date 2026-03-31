--
-- Test NULL handling optimizations (NO_NULLS, SPARSE_NULLS, RLE_NULLS)
-- Verifies that NULL bitmap is omitted or optimized based on NULL density.
--

-- Test 1: NO_NULLS optimization (column has zero NULLs)
CREATE TABLE noxu_no_nulls_test (
    id int NOT NULL,
    value text NOT NULL,
    amount int NOT NULL
) USING noxu;

INSERT INTO noxu_no_nulls_test
SELECT i, 'value_' || i, i * 10
FROM generate_series(1, 100) i;

SELECT COUNT(*) FROM noxu_no_nulls_test;
SELECT * FROM noxu_no_nulls_test WHERE id <= 5 ORDER BY id;

DROP TABLE noxu_no_nulls_test;

-- Test 2: SPARSE_NULLS optimization (<5% NULL density)
CREATE TABLE noxu_sparse_nulls_test (
    id int,
    value text,
    amount int
) USING noxu;

-- Insert 95 non-NULL rows and 5 NULL rows
INSERT INTO noxu_sparse_nulls_test
SELECT i, 'value_' || i, i * 10
FROM generate_series(1, 95) i;

INSERT INTO noxu_sparse_nulls_test VALUES
    (96, NULL, 960),
    (97, 'value_97', NULL),
    (98, NULL, NULL),
    (99, 'value_99', 990),
    (100, NULL, 1000);

SELECT COUNT(*) FROM noxu_sparse_nulls_test WHERE value IS NULL;
SELECT COUNT(*) FROM noxu_sparse_nulls_test WHERE amount IS NULL;
SELECT * FROM noxu_sparse_nulls_test WHERE value IS NULL ORDER BY id;

DROP TABLE noxu_sparse_nulls_test;

-- Test 3: RLE_NULLS optimization (sequential NULLs)
CREATE TABLE noxu_rle_nulls_test (
    id int,
    value text
) USING noxu;

-- Insert pattern: 10 values, 20 NULLs, 10 values, 30 NULLs
INSERT INTO noxu_rle_nulls_test
SELECT i, 'value_' || i
FROM generate_series(1, 10) i;

INSERT INTO noxu_rle_nulls_test
SELECT i, NULL
FROM generate_series(11, 30) i;

INSERT INTO noxu_rle_nulls_test
SELECT i, 'value_' || i
FROM generate_series(31, 40) i;

INSERT INTO noxu_rle_nulls_test
SELECT i, NULL
FROM generate_series(41, 70) i;

SELECT COUNT(*) FROM noxu_rle_nulls_test WHERE value IS NULL;
SELECT COUNT(*) FROM noxu_rle_nulls_test WHERE value IS NOT NULL;
SELECT * FROM noxu_rle_nulls_test WHERE id IN (9, 10, 11, 12, 29, 30, 31, 32) ORDER BY id;

DROP TABLE noxu_rle_nulls_test;

-- Test 4: High NULL density (50%+)
CREATE TABLE noxu_high_nulls_test (
    id int,
    value text
) USING noxu;

-- Insert alternating NULL and non-NULL
INSERT INTO noxu_high_nulls_test
SELECT i,
       CASE WHEN i % 2 = 0 THEN 'value_' || i ELSE NULL END
FROM generate_series(1, 100) i;

SELECT COUNT(*) FROM noxu_high_nulls_test WHERE value IS NULL;
SELECT COUNT(*) FROM noxu_high_nulls_test WHERE value IS NOT NULL;

DROP TABLE noxu_high_nulls_test;

-- Test 5: Very high NULL density (95%) - should use standard bitmap
CREATE TABLE noxu_mostly_nulls_test (
    id int,
    value text
) USING noxu;

-- Insert 100 rows: only 5 non-NULL, 95 NULL
INSERT INTO noxu_mostly_nulls_test
SELECT i,
       CASE WHEN i IN (10, 25, 50, 75, 90) THEN 'value_' || i ELSE NULL END
FROM generate_series(1, 100) i;

SELECT COUNT(*) FROM noxu_mostly_nulls_test WHERE value IS NULL;
SELECT COUNT(*) FROM noxu_mostly_nulls_test WHERE value IS NOT NULL;
SELECT * FROM noxu_mostly_nulls_test WHERE value IS NOT NULL ORDER BY id;

DROP TABLE noxu_mostly_nulls_test;

-- Test 6: Large-scale RLE test (bulk insert to ensure items pack together)
CREATE TABLE noxu_rle_bulk_test (
    id int,
    value int
) USING noxu;

-- Insert a single bulk batch: 500 non-NULL, 500 NULL, 500 non-NULL
-- This ensures the data lands in the same attribute items for RLE encoding.
INSERT INTO noxu_rle_bulk_test
SELECT i,
       CASE WHEN i <= 500 THEN i
	    WHEN i > 1000 THEN i
	    ELSE NULL END
FROM generate_series(1, 1500) i;

SELECT COUNT(*) FROM noxu_rle_bulk_test WHERE value IS NULL;
SELECT COUNT(*) FROM noxu_rle_bulk_test WHERE value IS NOT NULL;

-- Verify boundary values at NULL/non-NULL transitions
SELECT * FROM noxu_rle_bulk_test WHERE id IN (499, 500, 501, 502, 999, 1000, 1001, 1002) ORDER BY id;

DROP TABLE noxu_rle_bulk_test;

-- Test 7: Mixed NULL densities across columns in the same table
CREATE TABLE noxu_mixed_nulls_test (
    id int,
    always_set int,       -- 0% NULLs -> NO_NULLS
    rarely_null int,      -- ~2% NULLs -> SPARSE_NULLS
    half_null int,        -- 50% NULLs -> standard bitmap
    mostly_null int       -- 95% NULLs -> standard bitmap
) USING noxu;

INSERT INTO noxu_mixed_nulls_test
SELECT i,
       i * 10,
       CASE WHEN i % 50 = 0 THEN NULL ELSE i END,
       CASE WHEN i % 2 = 0 THEN NULL ELSE i END,
       CASE WHEN i % 20 = 0 THEN i ELSE NULL END
FROM generate_series(1, 1000) i;

SELECT COUNT(*) FROM noxu_mixed_nulls_test WHERE always_set IS NULL;
SELECT COUNT(*) FROM noxu_mixed_nulls_test WHERE rarely_null IS NULL;
SELECT COUNT(*) FROM noxu_mixed_nulls_test WHERE half_null IS NULL;
SELECT COUNT(*) FROM noxu_mixed_nulls_test WHERE mostly_null IS NULL;

-- Verify a few specific rows across all columns
SELECT * FROM noxu_mixed_nulls_test WHERE id IN (1, 50, 100, 500, 1000) ORDER BY id;

DROP TABLE noxu_mixed_nulls_test;

-- Test 8: UPDATE and DELETE with NULL-optimized storage
CREATE TABLE noxu_null_mvcc_test (
    id int,
    value text
) USING noxu;

-- Start with all non-NULLs (should use NO_NULLS encoding)
INSERT INTO noxu_null_mvcc_test
SELECT i, 'value_' || i FROM generate_series(1, 50) i;

SELECT COUNT(*) FROM noxu_null_mvcc_test WHERE value IS NOT NULL;

-- Update some rows to NULL (forces re-encoding from NO_NULLS to a NULL-aware format)
UPDATE noxu_null_mvcc_test SET value = NULL WHERE id IN (10, 20, 30);
SELECT COUNT(*) FROM noxu_null_mvcc_test WHERE value IS NULL;
SELECT * FROM noxu_null_mvcc_test WHERE id IN (9, 10, 11, 19, 20, 21) ORDER BY id;

-- Delete rows and verify remaining data integrity
DELETE FROM noxu_null_mvcc_test WHERE id > 40;
SELECT COUNT(*) FROM noxu_null_mvcc_test;
SELECT * FROM noxu_null_mvcc_test WHERE id >= 38 ORDER BY id;

DROP TABLE noxu_null_mvcc_test;
