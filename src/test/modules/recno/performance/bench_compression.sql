--
-- bench_compression.sql
--
-- Measures compression effectiveness across data types.
-- Validates design doc claim: 20-40% space savings.
--
-- Output: Table sizes for RECNO (compressed) vs HEAP (uncompressed)
-- for integers, text, numeric, mixed, and high-entropy data.
--

-- Ensure compression is enabled
SET recno_enable_compression = on;

-- ======================================================================
-- Test 1: Integer column compression (delta encoding target)
-- Sequential integers are ideal for delta encoding.
-- ======================================================================
\echo '=== Test 1: Integer Column - Sequential (delta encoding) ==='

DROP TABLE IF EXISTS recno_comp_int CASCADE;
DROP TABLE IF EXISTS heap_comp_int CASCADE;

CREATE TABLE heap_comp_int (
    id      INT4,
    val_i4  INT4,
    val_i8  INT8
) USING heap;

CREATE TABLE recno_comp_int (
    id      INT4,
    val_i4  INT4,
    val_i8  INT8
) USING recno;

-- Insert 100K rows with sequential integer patterns (ideal for delta)
INSERT INTO heap_comp_int
SELECT i, i * 7 + 42, i::bigint * 100000 + 999999
FROM generate_series(1, 100000) i;

INSERT INTO recno_comp_int
SELECT i, i * 7 + 42, i::bigint * 100000 + 999999
FROM generate_series(1, 100000) i;

ANALYZE heap_comp_int;
ANALYZE recno_comp_int;

SELECT
    'Integer Sequential' AS test,
    'heap' AS am,
    pg_size_pretty(pg_relation_size('heap_comp_int')) AS table_size,
    pg_relation_size('heap_comp_int') AS size_bytes
UNION ALL
SELECT
    'Integer Sequential',
    'recno',
    pg_size_pretty(pg_relation_size('recno_comp_int')),
    pg_relation_size('recno_comp_int');

-- Verify data integrity
SELECT
    'heap' AS am,
    count(*) AS rows,
    sum(val_i4) AS checksum_i4,
    sum(val_i8) AS checksum_i8
FROM heap_comp_int
UNION ALL
SELECT
    'recno',
    count(*),
    sum(val_i4),
    sum(val_i8)
FROM recno_comp_int;

-- ======================================================================
-- Test 2: Text column compression (dictionary / LZ4 target)
-- Repetitive text benefits from dictionary compression.
-- ======================================================================
\echo '=== Test 2: Text Column - Repetitive (dictionary compression) ==='

DROP TABLE IF EXISTS recno_comp_text CASCADE;
DROP TABLE IF EXISTS heap_comp_text CASCADE;

CREATE TABLE heap_comp_text (
    id   INT4,
    body TEXT
) USING heap;

CREATE TABLE recno_comp_text (
    id   INT4,
    body TEXT
) USING recno;

-- Insert 50K rows of moderately repetitive text (~200 bytes each)
INSERT INTO heap_comp_text
SELECT i,
       'Customer order #' || i || ' placed on 2025-01-' ||
       lpad((i % 28 + 1)::text, 2, '0') ||
       '. Product: Widget-' || (i % 50) ||
       ', Qty: ' || (i % 100 + 1) ||
       ', Status: ' || (CASE i % 5
           WHEN 0 THEN 'pending'
           WHEN 1 THEN 'shipped'
           WHEN 2 THEN 'delivered'
           WHEN 3 THEN 'returned'
           ELSE 'cancelled' END) ||
       '. Notes: Standard fulfillment process applies.'
FROM generate_series(1, 50000) i;

INSERT INTO recno_comp_text
SELECT i,
       'Customer order #' || i || ' placed on 2025-01-' ||
       lpad((i % 28 + 1)::text, 2, '0') ||
       '. Product: Widget-' || (i % 50) ||
       ', Qty: ' || (i % 100 + 1) ||
       ', Status: ' || (CASE i % 5
           WHEN 0 THEN 'pending'
           WHEN 1 THEN 'shipped'
           WHEN 2 THEN 'delivered'
           WHEN 3 THEN 'returned'
           ELSE 'cancelled' END) ||
       '. Notes: Standard fulfillment process applies.'
FROM generate_series(1, 50000) i;

ANALYZE heap_comp_text;
ANALYZE recno_comp_text;

SELECT
    'Text Repetitive' AS test,
    'heap' AS am,
    pg_size_pretty(pg_relation_size('heap_comp_text')) AS table_size,
    pg_relation_size('heap_comp_text') AS size_bytes
UNION ALL
SELECT
    'Text Repetitive',
    'recno',
    pg_size_pretty(pg_relation_size('recno_comp_text')),
    pg_relation_size('recno_comp_text');

-- Verify data integrity
SELECT
    'heap' AS am,
    count(*) AS rows,
    sum(length(body)) AS total_text_bytes,
    md5(string_agg(body, '' ORDER BY id)) AS content_hash
FROM heap_comp_text
UNION ALL
SELECT
    'recno',
    count(*),
    sum(length(body)),
    md5(string_agg(body, '' ORDER BY id))
FROM recno_comp_text;

-- ======================================================================
-- Test 3: Highly repetitive text (best case for compression)
-- ======================================================================
\echo '=== Test 3: Highly Repetitive Text (best case) ==='

DROP TABLE IF EXISTS recno_comp_repeat CASCADE;
DROP TABLE IF EXISTS heap_comp_repeat CASCADE;

CREATE TABLE heap_comp_repeat (
    id   INT4,
    body TEXT
) USING heap;

CREATE TABLE recno_comp_repeat (
    id   INT4,
    body TEXT
) USING recno;

-- Insert 10K rows of 5KB highly repetitive text
INSERT INTO heap_comp_repeat
SELECT i, repeat('The quick brown fox jumps over the lazy dog. ', 100)
FROM generate_series(1, 10000) i;

INSERT INTO recno_comp_repeat
SELECT i, repeat('The quick brown fox jumps over the lazy dog. ', 100)
FROM generate_series(1, 10000) i;

SELECT
    'Text Highly Repetitive' AS test,
    'heap' AS am,
    pg_size_pretty(pg_relation_size('heap_comp_repeat')) AS table_size,
    pg_relation_size('heap_comp_repeat') AS size_bytes
UNION ALL
SELECT
    'Text Highly Repetitive',
    'recno',
    pg_size_pretty(pg_relation_size('recno_comp_repeat')),
    pg_relation_size('recno_comp_repeat');

-- ======================================================================
-- Test 4: NUMERIC column compression (delta encoding target)
-- Monotonic NUMERIC sequences should compress well with delta.
-- ======================================================================
\echo '=== Test 4: NUMERIC Column - Monotonic (delta encoding) ==='

DROP TABLE IF EXISTS recno_comp_numeric CASCADE;
DROP TABLE IF EXISTS heap_comp_numeric CASCADE;

CREATE TABLE heap_comp_numeric (
    id   INT4,
    price NUMERIC(12,2),
    qty   NUMERIC(8,0)
) USING heap;

CREATE TABLE recno_comp_numeric (
    id   INT4,
    price NUMERIC(12,2),
    qty   NUMERIC(8,0)
) USING recno;

-- Insert 100K rows with slowly-varying numeric values (good delta target)
INSERT INTO heap_comp_numeric
SELECT i,
       1000.00 + (i % 100) * 0.50 + (i / 1000) * 10.00,
       (i % 200) + 1
FROM generate_series(1, 100000) i;

INSERT INTO recno_comp_numeric
SELECT i,
       1000.00 + (i % 100) * 0.50 + (i / 1000) * 10.00,
       (i % 200) + 1
FROM generate_series(1, 100000) i;

ANALYZE heap_comp_numeric;
ANALYZE recno_comp_numeric;

SELECT
    'Numeric Monotonic' AS test,
    'heap' AS am,
    pg_size_pretty(pg_relation_size('heap_comp_numeric')) AS table_size,
    pg_relation_size('heap_comp_numeric') AS size_bytes
UNION ALL
SELECT
    'Numeric Monotonic',
    'recno',
    pg_size_pretty(pg_relation_size('recno_comp_numeric')),
    pg_relation_size('recno_comp_numeric');

-- Verify data integrity
SELECT
    'heap' AS am,
    count(*) AS rows,
    sum(price) AS checksum_price,
    sum(qty) AS checksum_qty
FROM heap_comp_numeric
UNION ALL
SELECT
    'recno',
    count(*),
    sum(price),
    sum(qty)
FROM recno_comp_numeric;

-- ======================================================================
-- Test 5: High-entropy data (worst case - should NOT compress)
-- Random bytes should not compress; verify no overhead penalty.
-- ======================================================================
\echo '=== Test 5: High Entropy Data (worst case) ==='

DROP TABLE IF EXISTS recno_comp_entropy CASCADE;
DROP TABLE IF EXISTS heap_comp_entropy CASCADE;

CREATE TABLE heap_comp_entropy (
    id   INT4,
    data BYTEA
) USING heap;

CREATE TABLE recno_comp_entropy (
    id   INT4,
    data BYTEA
) USING recno;

-- Insert 10K rows of 128-byte random data (incompressible)
INSERT INTO heap_comp_entropy
SELECT i, decode(md5(random()::text) || md5(random()::text) ||
                 md5(random()::text) || md5(random()::text), 'hex')
FROM generate_series(1, 10000) i;

INSERT INTO recno_comp_entropy
SELECT i, decode(md5(random()::text) || md5(random()::text) ||
                 md5(random()::text) || md5(random()::text), 'hex')
FROM generate_series(1, 10000) i;

SELECT
    'High Entropy (random)' AS test,
    'heap' AS am,
    pg_size_pretty(pg_relation_size('heap_comp_entropy')) AS table_size,
    pg_relation_size('heap_comp_entropy') AS size_bytes
UNION ALL
SELECT
    'High Entropy (random)',
    'recno',
    pg_size_pretty(pg_relation_size('recno_comp_entropy')),
    pg_relation_size('recno_comp_entropy');

-- ======================================================================
-- Test 6: Mixed-type table (realistic workload)
-- ======================================================================
\echo '=== Test 6: Mixed-Type Table (realistic) ==='

DROP TABLE IF EXISTS recno_comp_mixed CASCADE;
DROP TABLE IF EXISTS heap_comp_mixed CASCADE;

CREATE TABLE heap_comp_mixed (
    id          INT4,
    customer_id INT8,
    amount      NUMERIC(12,2),
    status      TEXT,
    description TEXT,
    created_at  TIMESTAMP
) USING heap;

CREATE TABLE recno_comp_mixed (
    id          INT4,
    customer_id INT8,
    amount      NUMERIC(12,2),
    status      TEXT,
    description TEXT,
    created_at  TIMESTAMP
) USING recno;

INSERT INTO heap_comp_mixed
SELECT i,
       1000000 + (i % 10000),
       (random() * 10000)::numeric(12,2),
       (CASE i % 4
           WHEN 0 THEN 'active'
           WHEN 1 THEN 'pending'
           WHEN 2 THEN 'complete'
           ELSE 'cancelled' END),
       'Order placed by customer ' || (1000000 + (i % 10000)) ||
       ' for product category ' || (i % 20) ||
       '. Shipping method: ' || (CASE i % 3
           WHEN 0 THEN 'standard'
           WHEN 1 THEN 'express'
           ELSE 'overnight' END),
       '2025-01-01'::timestamp + (i || ' seconds')::interval
FROM generate_series(1, 100000) i;

INSERT INTO recno_comp_mixed
SELECT i,
       1000000 + (i % 10000),
       (random() * 10000)::numeric(12,2),
       (CASE i % 4
           WHEN 0 THEN 'active'
           WHEN 1 THEN 'pending'
           WHEN 2 THEN 'complete'
           ELSE 'cancelled' END),
       'Order placed by customer ' || (1000000 + (i % 10000)) ||
       ' for product category ' || (i % 20) ||
       '. Shipping method: ' || (CASE i % 3
           WHEN 0 THEN 'standard'
           WHEN 1 THEN 'express'
           ELSE 'overnight' END),
       '2025-01-01'::timestamp + (i || ' seconds')::interval
FROM generate_series(1, 100000) i;

ANALYZE heap_comp_mixed;
ANALYZE recno_comp_mixed;

SELECT
    'Mixed Realistic' AS test,
    'heap' AS am,
    pg_size_pretty(pg_relation_size('heap_comp_mixed')) AS table_size,
    pg_relation_size('heap_comp_mixed') AS size_bytes,
    pg_size_pretty(pg_total_relation_size('heap_comp_mixed')) AS total_size
UNION ALL
SELECT
    'Mixed Realistic',
    'recno',
    pg_size_pretty(pg_relation_size('recno_comp_mixed')),
    pg_relation_size('recno_comp_mixed'),
    pg_size_pretty(pg_total_relation_size('recno_comp_mixed'));

-- ======================================================================
-- Compression Ratio Summary
-- ======================================================================
\echo '=== Compression Ratio Summary ==='

SELECT
    test_name,
    heap_bytes,
    recno_bytes,
    CASE WHEN heap_bytes > 0
         THEN round(100.0 * (1.0 - recno_bytes::numeric / heap_bytes), 1)
         ELSE 0
    END AS savings_pct,
    CASE WHEN recno_bytes > 0
         THEN round(heap_bytes::numeric / recno_bytes, 2)
         ELSE 0
    END AS compression_ratio
FROM (
    SELECT 'Integer Sequential' AS test_name,
           pg_relation_size('heap_comp_int') AS heap_bytes,
           pg_relation_size('recno_comp_int') AS recno_bytes
    UNION ALL
    SELECT 'Text Repetitive',
           pg_relation_size('heap_comp_text'),
           pg_relation_size('recno_comp_text')
    UNION ALL
    SELECT 'Text Highly Repetitive',
           pg_relation_size('heap_comp_repeat'),
           pg_relation_size('recno_comp_repeat')
    UNION ALL
    SELECT 'Numeric Monotonic',
           pg_relation_size('heap_comp_numeric'),
           pg_relation_size('recno_comp_numeric')
    UNION ALL
    SELECT 'High Entropy (random)',
           pg_relation_size('heap_comp_entropy'),
           pg_relation_size('recno_comp_entropy')
    UNION ALL
    SELECT 'Mixed Realistic',
           pg_relation_size('heap_comp_mixed'),
           pg_relation_size('recno_comp_mixed')
) sub
ORDER BY test_name;
