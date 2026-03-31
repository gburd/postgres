CREATE TABLE t_btree_concurrency(a int, b text) USING noxu;
CREATE INDEX ON t_btree_concurrency(a);
INSERT INTO t_btree_concurrency SELECT i, 'data' || i FROM generate_series(1, 5000) i;
SELECT COUNT(*) FROM t_btree_concurrency;
SELECT MIN(a), MAX(a) FROM t_btree_concurrency WHERE a > 2500;
DELETE FROM t_btree_concurrency WHERE a % 3 = 0;
INSERT INTO t_btree_concurrency SELECT i, 'reinsert' || i FROM generate_series(5001, 6000) i;
SELECT COUNT(*) FROM t_btree_concurrency;
SELECT COUNT(*) FROM t_btree_concurrency WHERE b LIKE 'reinsert%';
DROP TABLE t_btree_concurrency;

-- Test that btree root splits succeed with downlink page-fit validation.
-- Insert enough rows to force multiple levels of internal page splits,
-- exercising the nxbt_newroot() downlink overflow check.
CREATE TABLE t_btree_root_splits(id int, payload text) USING noxu;
INSERT INTO t_btree_root_splits
  SELECT i, repeat('x', 100) FROM generate_series(1, 50000) i;
SELECT COUNT(*) FROM t_btree_root_splits;
SELECT MIN(id), MAX(id) FROM t_btree_root_splits;
DROP TABLE t_btree_root_splits;
