# A BARK index built over a broken HOT chain must not be used by older
# snapshots.
#
# s1 takes a REPEATABLE READ snapshot without touching the table.  s2 then
# updates k on 20 rows; no index covers k yet, so the updates are HOT, and
# the old versions s1 still sees are RECENTLY_DEAD members of HOT chains
# whose live member has another k.  An index on k can only point at the
# chain's live member, so the table AM leaves the old versions out and
# reports a broken HOT chain (ii_BrokenHotChain), and index.c sets
# pg_index.indcheckxmin: s1, whose snapshot is older than the index, must
# not use it.  If it did, its index scans would find none of the 20 rows its
# sequential scan sees.
#
# Serial build: the table AM sets the flag in the IndexInfo the build passes
# it.  Parallel build: every participant, the leader included, scans with an
# IndexInfo of its own, so the flag reaches index.c only if the leader
# copies it back from the shared state.  Parallel REINDEX of an index marked
# indcheckxmin must keep the mark while the chains are still broken.
# CREATE INDEX CONCURRENTLY marks nothing: it waits for s1 to finish before
# the index becomes valid, and s1 cannot use the unfinished index meanwhile.

setup
{
	CREATE EXTENSION amcheck;
	CREATE TABLE bhc (id int, k int, v text)
		WITH (fillfactor = 50, autovacuum_enabled = off);
	INSERT INTO bhc SELECT g, g, repeat('x', 50)
		FROM generate_series(1, 10000) g;
}
setup
{
	VACUUM bhc;
}

teardown
{
	DROP TABLE bhc;
	DROP EXTENSION amcheck;
}

session s1
step s1_begin
{
	BEGIN ISOLATION LEVEL REPEATABLE READ;
	SELECT 1 AS snapshot;
}
step s1_count
{
	SET LOCAL enable_seqscan = off;
	SET LOCAL enable_bitmapscan = off;
	SELECT count(*) AS forced_index_count FROM bhc WHERE k <= 20;
	SET LOCAL enable_seqscan = on;
	SET LOCAL enable_indexscan = off;
	SET LOCAL enable_indexonlyscan = off;
	SELECT count(*) AS seqscan_count FROM bhc WHERE k <= 20;
}
step s1_commit	{ COMMIT; }

session s2
setup
{
	SET max_parallel_maintenance_workers = 2;
	SET min_parallel_table_scan_size = 0;
}
step s2_update	{ UPDATE bhc SET k = k + 100000 WHERE id <= 20; }
step s2_build_serial
{
	BEGIN;
	SET LOCAL max_parallel_maintenance_workers = 0;
	CREATE INDEX bhc_k ON bhc USING bark (k);
	COMMIT;
}
step s2_build_par	{ CREATE INDEX bhc_k ON bhc USING bark (k); }
step s2_reindex_par	{ REINDEX INDEX bhc_k; }
step s2_cic_par		{ CREATE INDEX CONCURRENTLY bhc_k ON bhc USING bark (k); }
step s2_flag
{
	SELECT indisvalid, indcheckxmin FROM pg_index
	 WHERE indexrelid = 'bhc_k'::regclass;
}
step s2_check
{
	SELECT bark_index_check('bhc_k', true);
	BEGIN;
	SET LOCAL enable_seqscan = off;
	SET LOCAL enable_bitmapscan = off;
	SELECT count(*) AS index_count, sum(k) AS index_sum FROM bhc
	 WHERE k <= 20 OR k > 100000;
	SET LOCAL enable_seqscan = on;
	SET LOCAL enable_indexscan = off;
	SET LOCAL enable_indexonlyscan = off;
	SELECT count(*) AS seqscan_count, sum(k) AS seqscan_sum FROM bhc
	 WHERE k <= 20 OR k > 100000;
	COMMIT;
}

permutation s1_begin s2_update s2_build_serial s2_flag s1_count s1_commit s2_check
permutation s1_begin s2_update s2_build_par s2_flag s1_count s1_commit s2_check
permutation s1_begin s2_update s2_build_serial s2_reindex_par s2_flag s1_count
	s1_commit s2_check
# s2_cic_par waits for s1's snapshot; the step after s1_commit runs in s2 so
# that it waits for the build to finish.
permutation s1_begin s2_update s2_cic_par s1_count s1_commit s2_flag s2_check
