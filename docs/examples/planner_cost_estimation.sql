-- planner_cost_estimation.sql
--
-- Demonstrates Orvos planner integration for columnar cost estimation.
-- Run after building PostgreSQL with Orvos support.
--
-- The key insight: queries that access few columns of a wide Orvos table
-- should show lower cost estimates than equivalent heap queries, because
-- Orvos reads only the columns needed.

------------------------------------------------------------------------
-- Setup: Create a wide table with 10 columns using both heap and orvos
------------------------------------------------------------------------

DROP TABLE IF EXISTS wide_orvos CASCADE;
DROP TABLE IF EXISTS wide_heap CASCADE;

-- Orvos columnar table
CREATE TABLE wide_orvos(
    col1  int,
    col2  int,
    col3  text,
    col4  numeric,
    col5  timestamp,
    col6  jsonb,
    col7  int,
    col8  text,
    col9  int,
    col10 text
) USING orvos;

-- Equivalent heap table for comparison
CREATE TABLE wide_heap(
    col1  int,
    col2  int,
    col3  text,
    col4  numeric,
    col5  timestamp,
    col6  jsonb,
    col7  int,
    col8  text,
    col9  int,
    col10 text
) USING heap;

------------------------------------------------------------------------
-- Load test data (100k rows with variable-width columns)
------------------------------------------------------------------------

INSERT INTO wide_orvos
SELECT
    i,
    i % 1000,
    repeat('x', 100),
    i * 1.5,
    now() - (i || ' seconds')::interval,
    ('{"key": ' || i || '}')::jsonb,
    i % 500,
    repeat('y', 50),
    i % 100,
    repeat('z', 75)
FROM generate_series(1, 100000) i;

INSERT INTO wide_heap SELECT * FROM wide_orvos;

------------------------------------------------------------------------
-- Collect statistics
------------------------------------------------------------------------

ANALYZE wide_orvos;
ANALYZE wide_heap;

------------------------------------------------------------------------
-- Example 1: Narrow projection (2 of 10 columns)
--
-- Orvos should estimate fewer pages because it only reads col1 and col3.
-- Heap always reads entire rows regardless of projection.
------------------------------------------------------------------------

\echo '=== Example 1: Narrow projection (2/10 columns) ==='

\echo '--- Orvos: SELECT col1, col3 ---'
EXPLAIN (COSTS ON) SELECT col1, col3 FROM wide_orvos WHERE col1 < 1000;

\echo '--- Heap: SELECT col1, col3 ---'
EXPLAIN (COSTS ON) SELECT col1, col3 FROM wide_heap WHERE col1 < 1000;

------------------------------------------------------------------------
-- Example 2: Wide projection (all columns)
--
-- SELECT * accesses all columns. The planner should not reduce Orvos
-- costs when column selectivity >= 80%.
------------------------------------------------------------------------

\echo '=== Example 2: Wide projection (all columns) ==='

\echo '--- Orvos: SELECT * ---'
EXPLAIN (COSTS ON) SELECT * FROM wide_orvos WHERE col1 < 1000;

\echo '--- Heap: SELECT * ---'
EXPLAIN (COSTS ON) SELECT * FROM wide_heap WHERE col1 < 1000;

------------------------------------------------------------------------
-- Example 3: Single-column aggregation
--
-- Analytical queries that aggregate one column benefit most from
-- columnar storage. Compare the cost of avg() on a single column.
------------------------------------------------------------------------

\echo '=== Example 3: Single-column aggregation ==='

\echo '--- Orvos: avg(col4) ---'
EXPLAIN (COSTS ON) SELECT avg(col4) FROM wide_orvos;

\echo '--- Heap: avg(col4) ---'
EXPLAIN (COSTS ON) SELECT avg(col4) FROM wide_heap;

------------------------------------------------------------------------
-- Example 4: Cost comparison across projection widths
--
-- Shows how Orvos cost scales with the number of columns accessed.
-- More columns = higher cost, approaching heap equivalence.
------------------------------------------------------------------------

\echo '=== Example 4: Scaling with projection width ==='

\echo '--- 1 column ---'
EXPLAIN (COSTS ON) SELECT col1 FROM wide_orvos;

\echo '--- 3 columns ---'
EXPLAIN (COSTS ON) SELECT col1, col2, col3 FROM wide_orvos;

\echo '--- 5 columns ---'
EXPLAIN (COSTS ON) SELECT col1, col2, col3, col4, col5 FROM wide_orvos;

\echo '--- All 10 columns ---'
EXPLAIN (COSTS ON) SELECT * FROM wide_orvos;

------------------------------------------------------------------------
-- Cleanup
------------------------------------------------------------------------

-- DROP TABLE wide_orvos;
-- DROP TABLE wide_heap;
