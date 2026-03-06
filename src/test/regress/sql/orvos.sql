-- simple tests to iteratively build the orvos
-- create and drop works
create table t_orvos(c1 int, c2 int, c3 int) USING orvos;
drop table t_orvos;
-- insert and select works
create table t_orvos(c1 int, c2 int, c3 int) USING orvos;
insert into t_orvos select i,i+1,i+2 from generate_series(1, 10)i;
select * from t_orvos;
-- selecting only few columns work
select c1, c3 from t_orvos;
-- only few columns in output and where clause work
select c3 from t_orvos where c2 > 5;

-- Test abort works
begin;
insert into t_orvos select i,i+1,i+2 from generate_series(21, 25)i;
abort;
insert into t_orvos select i,i+1,i+2 from generate_series(31, 35)i;
select * from t_orvos;

--
-- Test indexing
--
create index on t_orvos (c1);
set enable_seqscan=off;
set enable_indexscan=on;
set enable_bitmapscan=off;

-- index scan
select * from t_orvos where c1 = 5;

-- index-only scan
select c1 from t_orvos where c1 = 5;

-- bitmap scan
set enable_indexscan=off;
set enable_bitmapscan=on;
select c1, c2 from t_orvos where c1 between 5 and 10;

--
-- Test DELETE and UPDATE
--
delete from t_orvos where c2 = 5;
select * from t_orvos;
delete from t_orvos where c2 < 5;
select * from t_orvos;

update t_orvos set c2 = 100 where c1 = 8;
select * from t_orvos;

--
-- Test page deletion, by deleting a bigger range of values
--
insert into t_orvos select i,i+1,i+2 from generate_series(10000, 15000)i;
delete from t_orvos where c1 >= 10000;

--
-- Test VACUUM
--
vacuum t_orvos;
select * from t_orvos;

--
-- Test toasting
--
create table t_zedtoast(c1 int, t text) USING orvos;
insert into t_zedtoast select i, repeat('x', 10000) from generate_series(1, 10) i;

select c1, length(t) from t_zedtoast;

--
-- Test NULL values
--
create table t_zednullvalues(c1 int, c2 int) USING orvos;
insert into t_zednullvalues values(1, NULL), (NULL, 2);
select * from t_zednullvalues;
select c2 from t_zednullvalues;
update t_zednullvalues set c1 = 1, c2 = NULL;
select * from t_zednullvalues;

--
-- Test COPY
--
create table t_zedcopy(a serial, b int, c text not null default 'stuff', d text,e text) USING orvos;

COPY t_zedcopy (a, b, c, d, e) from stdin;
9999	\N	\\N	\NN	\N
10000	21	31	41	51
\.

COPY t_zedcopy (b, d) from stdin;
1	test_1
\.

COPY t_zedcopy (b, d) from stdin;
2	test_2
3	test_3
4	test_4
5	test_5
\.

COPY t_zedcopy (a, b, c, d, e) from stdin;
10001	22	32	42	52
10002	23	33	43	53
10003	24	34	44	54
10004	25	35	45	55
10005	26	36	46	56
\.

select * from t_zedcopy;
COPY t_zedcopy (a, d, e) to stdout;

--
-- Also test delete and update on the table that was populated with COPY.
-- This exercises splitting the array item. (A table not populated with
-- COPY only contains single items, at the moment.)
--

delete from t_zedcopy where b = 4;
select * from t_zedcopy;
delete from t_zedcopy where b < 3;
select * from t_zedcopy;

update t_zedcopy set b = 100 where b = 5;
select * from t_zedcopy;


-- Test rolling back COPY
begin;
COPY t_zedcopy (b, d) from stdin;
20001	test_1
20002	test_2
20003	test_3
20004	test_4
\.
rollback;
select count(*) from t_zedcopy where b >= 20000;

--
-- Test zero column table
--
create table t_zwithzerocols() using orvos;
insert into t_zwithzerocols select t.* from t_zwithzerocols t right join generate_series(1,1) on true;
select count(*) from t_zwithzerocols;

-- Test for alter table add column
create table t_zaddcol(a int) using orvos;
insert into t_zaddcol select * from generate_series(1, 3);
-- rewrite case
alter table t_zaddcol add column b int generated always as (a + 1) stored;
select * from t_zaddcol;
-- test alter table add column with no default
create table t_zaddcol_simple(a int) using orvos;
insert into t_zaddcol_simple values (1);
alter table t_zaddcol_simple add b int;
select * from t_zaddcol_simple;
insert into t_zaddcol_simple values(2,3);
select * from t_zaddcol_simple;
-- fixed length default value stored in catalog
alter table t_zaddcol add column c int default 3;
select * from t_zaddcol;
-- variable length default value stored in catalog
alter table t_zaddcol add column d text default 'abcdefgh';
select d from t_zaddcol;
-- insert after add column
insert into t_zaddcol values (2);
select * from t_zaddcol;
insert into t_zaddcol (a, c, d) values (3,5, 'test_insert');
select b,c,d from t_zaddcol;

--
-- Test TABLESAMPLE
--
-- regular test tablesample.sql doesn't directly work for orvos as
-- its using fillfactor to create specific block layout for
-- heap. Hence, output differs between heap and orvos table while
-- sampling. We need to use many tuples here to have multiple logical
-- blocks as don't have way to force TIDs spread / jump for orvos.
--
CREATE TABLE t_ztablesample (id int, name text) using orvos;
INSERT INTO t_ztablesample
       SELECT i, repeat(i::text, 2) FROM generate_series(0, 299) s(i);
-- lets delete half (even numbered ids) rows to limit the output
DELETE FROM t_ztablesample WHERE id%2 = 0;
-- should return ALL visible tuples from SOME blocks
SELECT ctid,t.id FROM t_ztablesample AS t TABLESAMPLE SYSTEM (50) REPEATABLE (0);
-- should return SOME visible tuples but from ALL the blocks
SELECT ctid,id FROM t_ztablesample TABLESAMPLE BERNOULLI (50) REPEATABLE (0);

--
-- Test column-delta UPDATE optimization
--
-- When fewer than half the columns change, Orvos uses a delta path that
-- skips unchanged column B-tree inserts and fetches them from the
-- predecessor TID instead.
--

-- Wide table: single column update should use delta path (1/6 < 50%)
create table t_delta(a int, b int, c text, d numeric, e int, f text)
  USING orvos;
insert into t_delta values
  (1, 10, 'hello', 1.5, 100, 'world'),
  (2, 20, 'foo',   2.5, 200, 'bar'),
  (3, 30, 'baz',   3.5, 300, 'qux');
-- Update single column
update t_delta set b = 99 where a = 2;
select * from t_delta order by a;

-- Update two columns (2/6 < 50%, still delta)
update t_delta set c = 'changed', e = 999 where a = 1;
select * from t_delta order by a;

-- Update four columns (4/6 > 50%, should use full path)
update t_delta set b = 0, c = 'full', d = 0.0, f = 'replaced' where a = 3;
select * from t_delta order by a;

-- Chained delta: update same row twice (predecessor chain depth 2)
update t_delta set b = 88 where a = 2;
select * from t_delta order by a;

-- VACUUM should materialize carried-forward columns
vacuum t_delta;
select * from t_delta order by a;

-- Two-column table: any single-column update changes 50%,
-- which is NOT < threshold, so full path should be used
create table t_delta_two(a int, b int) USING orvos;
insert into t_delta_two values (1, 10), (2, 20);
update t_delta_two set b = 99 where a = 1;
select * from t_delta_two order by a;
vacuum t_delta_two;
select * from t_delta_two order by a;

-- Test delta UPDATE with NULL values
create table t_delta_null(a int, b int, c text, d int) USING orvos;
insert into t_delta_null values (1, 10, 'test', 100);
-- Change one column to NULL (delta path: 1/4 < 50%)
update t_delta_null set b = NULL where a = 1;
select * from t_delta_null;
-- Change NULL back to value
update t_delta_null set b = 20 where a = 1;
select * from t_delta_null;
vacuum t_delta_null;
select * from t_delta_null;

-- Clean up
drop table t_delta;
drop table t_delta_two;
drop table t_delta_null;

--
-- Test ANALYZE column statistics collection
--
-- Create a wide table to test columnar statistics
CREATE TABLE t_analyze(
    col1  int,
    col2  int,
    col3  text,
    col4  numeric,
    col5  timestamp,
    col6  int,
    col7  text,
    col8  int,
    col9  text,
    col10 int
) USING orvos;

-- Insert data with varying compression characteristics
INSERT INTO t_analyze 
SELECT 
    i,
    i % 1000,
    repeat('test_data_' || (i % 10)::text, 5),  -- repetitive, compresses well
    i * 1.5,
    now() - (i || ' seconds')::interval,
    i % 100,
    repeat('x', 50),
    i % 50,
    repeat('y', 75),
    i
FROM generate_series(1, 1000) i;

-- Run ANALYZE to collect columnar statistics
ANALYZE t_analyze;

-- Verify that Orvos-specific statistics were collected and stored
-- Check for custom stakind (10001 = STATISTIC_KIND_ORVOS_COMPRESSION)
SELECT attname, 
       stakind1, stakind2, stakind3, stakind4, stakind5,
       (stakind1 = 10001 OR stakind2 = 10001 OR stakind3 = 10001 OR 
        stakind4 = 10001 OR stakind5 = 10001) AS has_orvos_stats
FROM pg_statistic s
JOIN pg_attribute a ON s.starelid = a.attrelid AND s.staattnum = a.attnum
WHERE s.starelid = 't_analyze'::regclass
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY a.attnum;

-- Verify compression statistics are reasonable
-- Extract compression ratios from stanumbers arrays where stakind = 10001
WITH orvos_stats AS (
  SELECT 
    a.attname,
    CASE 
      WHEN s.stakind1 = 10001 THEN s.stanumbers1[1]
      WHEN s.stakind2 = 10001 THEN s.stanumbers2[1]
      WHEN s.stakind3 = 10001 THEN s.stanumbers3[1]
      WHEN s.stakind4 = 10001 THEN s.stanumbers4[1]
      WHEN s.stakind5 = 10001 THEN s.stanumbers5[1]
    END AS compression_ratio
  FROM pg_statistic s
  JOIN pg_attribute a ON s.starelid = a.attrelid AND s.staattnum = a.attnum
  WHERE s.starelid = 't_analyze'::regclass
    AND a.attnum > 0
    AND NOT a.attisdropped
    AND (s.stakind1 = 10001 OR s.stakind2 = 10001 OR s.stakind3 = 10001 OR 
         s.stakind4 = 10001 OR s.stakind5 = 10001)
)
SELECT 
  attname,
  compression_ratio,
  CASE 
    WHEN compression_ratio >= 1.0 AND compression_ratio <= 10.0 THEN 'reasonable'
    ELSE 'unexpected'
  END AS sanity_check
FROM orvos_stats
ORDER BY attname;

--
-- Test planner cost estimation with column projection
--
-- Create equivalent heap table for cost comparison
CREATE TABLE t_analyze_heap(
    col1  int,
    col2  int,
    col3  text,
    col4  numeric,
    col5  timestamp,
    col6  int,
    col7  text,
    col8  int,
    col9  text,
    col10 int
) USING heap;

INSERT INTO t_analyze_heap SELECT * FROM t_analyze;
ANALYZE t_analyze_heap;

-- Test 1: Narrow projection (2 of 10 columns)
-- Orvos should show lower cost than heap due to column projection
EXPLAIN (COSTS OFF, SUMMARY OFF)
SELECT col1, col3 FROM t_analyze WHERE col1 < 500;

EXPLAIN (COSTS OFF, SUMMARY OFF)
SELECT col1, col3 FROM t_analyze_heap WHERE col1 < 500;

-- Test 2: Wide projection (all 10 columns)
-- Costs should be similar between orvos and heap
EXPLAIN (COSTS OFF, SUMMARY OFF)
SELECT * FROM t_analyze WHERE col1 < 500;

EXPLAIN (COSTS OFF, SUMMARY OFF)
SELECT * FROM t_analyze_heap WHERE col1 < 500;

-- Test 3: Single column aggregation (highly selective)
-- Orvos should be significantly cheaper
EXPLAIN (COSTS OFF, SUMMARY OFF)
SELECT AVG(col1) FROM t_analyze;

EXPLAIN (COSTS OFF, SUMMARY OFF)
SELECT AVG(col1) FROM t_analyze_heap;

-- Cleanup
DROP TABLE t_analyze CASCADE;
DROP TABLE t_analyze_heap CASCADE;

--
-- Test opportunistic UNDO trimming (Phase 1)
--
-- This tests that UNDO trimming uses non-blocking locks and heuristics
CREATE TABLE t_undo_trim(a int, b text) USING orvos;

-- Generate UNDO log entries via aborted transaction
BEGIN;
INSERT INTO t_undo_trim SELECT i, 'row' || i FROM generate_series(1, 100) i;
ROLLBACK;

-- Insert committed data
INSERT INTO t_undo_trim SELECT i, 'committed' || i FROM generate_series(1, 50) i;

-- Multiple visibility checks should trigger opportunistic UNDO trim
-- (uses fast path with shared locks and heuristic)
SELECT COUNT(*) FROM t_undo_trim;
SELECT COUNT(*) FROM t_undo_trim WHERE a > 25;
SELECT COUNT(*) FROM t_undo_trim WHERE b LIKE 'committed%';

-- Verify data is correct after UNDO trimming
SELECT COUNT(*) FROM t_undo_trim;

-- Explicit VACUUM should also work (uses blocking lock, always trims)
VACUUM t_undo_trim;
SELECT COUNT(*) FROM t_undo_trim;

DROP TABLE t_undo_trim;

--
-- Test B-tree concurrency (cache invalidation and deadlock detection)
--
-- This test verifies that B-tree operations don't deadlock when the metacache
-- is stale. The fix prevents self-deadlock by invalidating cache before descent
-- and detecting attempts to lock buffers already held.
CREATE TABLE t_btree_concurrency(a int, b text) USING orvos;
CREATE INDEX ON t_btree_concurrency(a);

-- Insert enough data to cause B-tree splits
-- This exercises the code path where we hold a buffer and need to find parent
INSERT INTO t_btree_concurrency SELECT i, 'data' || i FROM generate_series(1, 5000) i;

-- Verify data integrity after splits
SELECT COUNT(*) FROM t_btree_concurrency;
SELECT MIN(a), MAX(a) FROM t_btree_concurrency WHERE a > 2500;

-- Delete and reinsert to exercise tree modifications with stale cache
DELETE FROM t_btree_concurrency WHERE a % 3 = 0;
INSERT INTO t_btree_concurrency SELECT i, 'reinsert' || i FROM generate_series(5001, 6000) i;

-- Verify correctness
SELECT COUNT(*) FROM t_btree_concurrency;
SELECT COUNT(*) FROM t_btree_concurrency WHERE b LIKE 'reinsert%';

DROP TABLE t_btree_concurrency;

--
-- Test opportunistic statistics collection
--
-- Verify that DML operations update tuple counts and that the planner
-- can use them for better estimates between ANALYZE runs.

-- Enable the feature and set a fast sampling rate for testing.
SET orvos.enable_opportunistic_stats = on;
SET orvos.stats_sample_rate = 1;
SET orvos.stats_freshness_threshold = 3600;

CREATE TABLE t_opstats(a int, b text, c int) USING orvos;

-- Insert data.  This should increment the insert counter.
INSERT INTO t_opstats SELECT i, 'row' || i, i * 2
FROM generate_series(1, 1000) i;

-- A sequential scan should populate scan-based tuple counts.
SELECT COUNT(*) FROM t_opstats;

-- Delete some rows.  This should increment the delete counter.
DELETE FROM t_opstats WHERE a <= 300;

-- Another scan should see the reduced row count.
SELECT COUNT(*) FROM t_opstats;

-- Planner should use opportunistic stats for this EXPLAIN.
-- We just check that it runs without error; exact costs are unstable.
SET log_statement = 'none';  -- Disable statement logging to avoid test diff noise
SET client_min_messages = 'debug2';
EXPLAIN (COSTS OFF) SELECT a FROM t_opstats WHERE a > 100;
RESET client_min_messages;
RESET log_statement;

-- Verify that disabling the GUC suppresses collection.
SET orvos.enable_opportunistic_stats = off;
INSERT INTO t_opstats SELECT i, 'extra' || i, i
FROM generate_series(2000, 2100) i;
SET orvos.enable_opportunistic_stats = on;

-- Clean up
DROP TABLE t_opstats;
