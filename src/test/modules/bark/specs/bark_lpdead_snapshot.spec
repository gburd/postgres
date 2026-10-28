# A dead-entry hint never hides a row from a snapshot that can see it.
#
# An index scan marks an entry LP_DEAD only when the table AM finds the
# entry's row dead to every snapshot, and later scans skip marked entries.
# s1 holds a REPEATABLE READ snapshot taken before s2 deletes rows.  s2's
# index scans find the rows dead to its own snapshot but not to s1's, so
# heap_hot_search_buffer reports them not all-dead and no entry is marked:
# s1's index scans still return them, as its sequential scan does.  Once s1
# commits, s2's next scan marks the entries, and a new snapshot in s1 does
# not see the rows.  The LIST entry of key 2, one of whose rows s2 deleted,
# stays unmarked, and the BARK-implemented btree index lpd_v is checked the
# same way.  A backend recomputes the removal horizon only when its
# snapshot's xmin has moved (GlobalVisTestShouldUpdate), so s2 takes an XID
# after s1 commits, or its next scan would still count s1 in.

setup
{
	CREATE EXTENSION IF NOT EXISTS pageinspect;
	CREATE TABLE lpd (k int, v int) WITH (autovacuum_enabled = off);
	INSERT INTO lpd SELECT g, g FROM generate_series(1, 200) g;
	INSERT INTO lpd SELECT 2, -g FROM generate_series(1, 20) g;
	CREATE INDEX lpd_k ON lpd USING bark (k);
	SET default_btree_implementation = bark;
	CREATE INDEX lpd_v ON lpd USING btree (v);
	RESET default_btree_implementation;
}

teardown
{
	DROP TABLE lpd;
}

session s1
setup
{
	SET enable_seqscan = off;
	SET enable_bitmapscan = off;
	SET enable_indexonlyscan = off;
}
step s1_begin
{
	BEGIN ISOLATION LEVEL REPEATABLE READ;
	SELECT 1 AS snapshot;
}
step s1_k
{
	SELECT count(*) AS k_rows, count(*) FILTER (WHERE k = 2) AS k2_rows
	  FROM lpd WHERE k BETWEEN 1 AND 50;
}
step s1_v
{
	SELECT count(*) AS v_rows FROM lpd WHERE v BETWEEN 1 AND 50;
}
step s1_seq
{
	SET LOCAL enable_seqscan = on;
	SET LOCAL enable_indexscan = off;
	SELECT count(*) AS seq_rows FROM lpd WHERE k BETWEEN 1 AND 50;
}
step s1_commit	{ COMMIT; }

session s2
setup
{
	SET enable_seqscan = off;
	SET enable_bitmapscan = off;
	SET enable_indexonlyscan = off;
}
step s2_delete
{
	DELETE FROM lpd WHERE k BETWEEN 1 AND 50 AND k <> 2;
	DELETE FROM lpd WHERE k = 2 AND v = -1;
}
step s2_scan
{
	SELECT count(*) AS k_rows FROM lpd WHERE k BETWEEN 1 AND 50;
	SELECT count(*) AS v_rows FROM lpd WHERE v BETWEEN 1 AND 50;
}
step s2_xid
{
	SELECT pg_current_xact_id() IS NOT NULL AS xid;
}
step s2_dead
{
	SELECT (SELECT sum(dead_items) FROM bark_multi_page_stats('lpd_k', 1, -1)
			 WHERE type = 'leaf') AS k_dead,
		   (SELECT sum(dead_items) FROM bark_multi_page_stats('lpd_v', 1, -1)
			 WHERE type = 'leaf') AS v_dead;
}

permutation
	s1_begin s1_k s2_delete s2_scan s2_dead s1_k s1_v s1_seq s1_commit
	s2_xid s2_scan s2_dead s1_begin s1_k s1_v s1_seq s1_commit
