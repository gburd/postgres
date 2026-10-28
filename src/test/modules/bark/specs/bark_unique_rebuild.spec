# CLUSTER, VACUUM FULL and REPACK rebuild a unique index without checking
# the constraint: reindex_index's skip_constraint_checks clears the
# IndexInfo's ii_Unique.  The rebuild still indexes the rows an older
# snapshot sees, here deleted rows whose keys were inserted again, and each
# must keep an entry of its own, as in any unique index: bark_check_unique
# refuses an entry of several heap TIDs as corruption, so a later insert of
# the key would fail with that instead of a unique violation, and a row
# deleted and inserted again would be lost.  s1 holds a REPEATABLE READ
# snapshot without touching the table, so the rebuilds do not wait for it.
# cu_a is a BARK index, cu_b a btree index that BARK implements.  After each
# rebuild the entries are all SINGLE, fresh keys insert, a deleted key
# inserts again, a duplicate of either key is a unique violation, index
# scans return the live rows, and bark_index_check, which reports a LIST or
# POSTING entry in a unique index, passes.  The parallel rebuild's workers
# only sort; the leader writes the entries.

setup
{
	CREATE EXTENSION IF NOT EXISTS amcheck;
	CREATE EXTENSION IF NOT EXISTS pageinspect;
	CREATE TABLE cu (a int, b int) WITH (autovacuum_enabled = off);
	CREATE UNIQUE INDEX cu_a ON cu USING bark (a);
	SET default_btree_implementation = bark;
	CREATE UNIQUE INDEX cu_b ON cu USING btree (b);
	RESET default_btree_implementation;
	INSERT INTO cu SELECT g, g FROM generate_series(1, 20) g;
}

teardown
{
	DROP TABLE cu;
}

session s1
step s1_begin
{
	BEGIN ISOLATION LEVEL REPEATABLE READ;
	SELECT 1 AS snapshot;
}
step s1_commit	{ COMMIT; }

session s2
setup
{
	SET enable_seqscan = off;
	SET enable_bitmapscan = off;
}
step s2_churn
{
	DELETE FROM cu WHERE a <= 5;
	INSERT INTO cu SELECT g, g FROM generate_series(1, 5) g;
}
step s2_cluster	{ CLUSTER cu USING cu_a; }
step s2_vacfull	{ VACUUM FULL cu; }
step s2_repack	{ REPACK cu; }
step s2_repack_b	{ REPACK cu USING INDEX cu_b; }
step s2_par
{
	SET max_parallel_maintenance_workers = 2;
	SET min_parallel_table_scan_size = 0;
}
step s2_shapes
{
	SELECT 'cu_a' AS idx, shape, count(*) FROM bark_page_items('cu_a', 1)
	  GROUP BY shape
	UNION ALL
	SELECT 'cu_b', shape, count(*) FROM bark_page_items('cu_b', 1)
	  GROUP BY shape;
}
step s2_insert	{ INSERT INTO cu SELECT g, g FROM generate_series(21, 30) g; }
step s2_reinsert
{
	DELETE FROM cu WHERE a = 4;
	INSERT INTO cu VALUES (4, 4);
}
step s2_dup_a	{ INSERT INTO cu VALUES (3, 1003); }
step s2_dup_b	{ INSERT INTO cu VALUES (1003, 3); }
step s2_scan
{
	EXPLAIN (COSTS OFF) SELECT count(*), sum(b) FROM cu WHERE a BETWEEN 1 AND 30;
	SELECT count(*), sum(b) FROM cu WHERE a BETWEEN 1 AND 30;
	EXPLAIN (COSTS OFF) SELECT count(*), sum(a) FROM cu WHERE b BETWEEN 1 AND 30;
	SELECT count(*), sum(a) FROM cu WHERE b BETWEEN 1 AND 30;
}
step s2_check
{
	SELECT bark_index_check('cu_a', true), bark_index_check('cu_b', true);
}

permutation s1_begin s2_churn s2_cluster s2_shapes s2_insert s2_reinsert s2_dup_a s2_dup_b s2_scan s2_check s1_commit
permutation s1_begin s2_churn s2_vacfull s2_shapes s2_insert s2_reinsert s2_dup_a s2_dup_b s2_scan s2_check s1_commit
permutation s1_begin s2_churn s2_repack s2_shapes s2_insert s2_reinsert s2_dup_a s2_dup_b s2_scan s2_check s1_commit
permutation s1_begin s2_churn s2_repack_b s2_shapes s2_insert s2_reinsert s2_dup_a s2_dup_b s2_scan s2_check s1_commit
permutation s1_begin s2_churn s2_par s2_vacfull s2_shapes s2_insert s2_reinsert s2_dup_a s2_dup_b s2_scan s2_check s1_commit
