# CREATE INDEX and REINDEX must index RECENTLY_DEAD rows.
#
# s1 takes a REPEATABLE READ snapshot.  s2 then deletes rows and updates
# others (non-HOT: h is indexed by a btree, so the old versions stay where
# they are), and builds a BARK index.  The table AM passes the versions s1
# still sees to the build with tupleIsAlive false, which means "do not
# unique-check", not "do not index": s1's index, index-only and bitmap scans
# of the new index must return what its sequential scan does.  s1 takes its
# snapshot without touching the tables, so it holds no lock REINDEX waits on.
#
# Unique: the old version of an updated row and a deleted row whose key was
# inserted again share their key with a live row, which must not fail the
# build (btree's does not) while a live duplicate still fails an insert; the
# deleted row 10000 sorts after every live one.
# Oversized: the keys go into the loaded tree by the build's second pass;
# filler space freed by VACUUM puts the live copy of a key before its dead
# copy in the heap, so the dead one would be the one checked.  Multikey:
# scans of such an index are refused, so the (key, TID) members of every
# row version s1 sees are compared with the index's members.  Parallel: the
# same rows through workers.  REINDEX CONCURRENTLY builds from an MVCC
# snapshot and waits for s1 before the new index is used, so s1 reads the
# old one meanwhile; the new one must hold every row a new snapshot sees.

setup
{
	CREATE EXTENSION amcheck;
	CREATE EXTENSION bark_multikey;
	CREATE TABLE rd (id int, k int, h int, v text);
	INSERT INTO rd SELECT g, g, g, repeat('x', 100)
		FROM generate_series(1, 10000) g;
	CREATE INDEX rd_h ON rd (h);
	CREATE TABLE rdm (id int, a int4[], h int);
	INSERT INTO rdm SELECT g, ARRAY[g, g + 1000, g % 7], g
		FROM generate_series(1, 200) g;
	CREATE INDEX rdm_h ON rdm (h);
	CREATE FUNCTION rd_big(int) RETURNS text LANGUAGE sql IMMUTABLE AS
		$$ SELECT 'K' || $1 || string_agg(md5($1::text || g::text), '')
			 FROM generate_series(1, 160) g $$;
	CREATE TABLE rdo (id int, k text);
	INSERT INTO rdo VALUES (0, 'filler');
	INSERT INTO rdo SELECT g, rd_big(g) FROM generate_series(1, 3) g;
	INSERT INTO rdo SELECT 100 + g, 'small' || g FROM generate_series(1, 3) g;
	DELETE FROM rdo WHERE id = 0;
}
setup
{
	VACUUM rd, rdm, rdo;
}

teardown
{
	DROP TABLE rd, rdm, rdo;
	DROP FUNCTION rd_big(int);
	DROP EXTENSION bark_multikey;
	DROP EXTENSION amcheck;
}

session s1
step s1_begin
{
	BEGIN ISOLATION LEVEL REPEATABLE READ;
	SELECT 1 AS snapshot;
}
step s1_idx
{
	SET LOCAL enable_seqscan = off;
	SET LOCAL enable_bitmapscan = off;
	EXPLAIN (COSTS OFF) SELECT count(*), sum(k) FROM rd WHERE id BETWEEN 1 AND 20;
	SELECT count(*), sum(k) FROM rd WHERE id BETWEEN 1 AND 20;
}
step s1_ios
{
	EXPLAIN (COSTS OFF) SELECT count(*), sum(id) FROM rd WHERE id BETWEEN 1 AND 20;
	SELECT count(*), sum(id) FROM rd WHERE id BETWEEN 1 AND 20;
}
step s1_bmp
{
	SET LOCAL enable_indexscan = off;
	SET LOCAL enable_bitmapscan = on;
	EXPLAIN (COSTS OFF) SELECT count(*), sum(k) FROM rd WHERE id BETWEEN 1 AND 20;
	SELECT count(*), sum(k) FROM rd WHERE id BETWEEN 1 AND 20;
}
step s1_seq
{
	SET LOCAL enable_seqscan = on;
	SET LOCAL enable_bitmapscan = off;
	EXPLAIN (COSTS OFF) SELECT count(*), sum(k) FROM rd WHERE id BETWEEN 1 AND 20;
	SELECT count(*), sum(k) FROM rd WHERE id BETWEEN 1 AND 20;
}
step s1_o_idx
{
	SET LOCAL enable_seqscan = off;
	SET LOCAL enable_bitmapscan = off;
	EXPLAIN (COSTS OFF) SELECT count(*), sum(id) FROM rdo WHERE k > '';
	SELECT count(*), sum(id) FROM rdo WHERE k > '';
}
step s1_o_ios
{
	EXPLAIN (COSTS OFF) SELECT count(*), sum(length(k)) FROM rdo WHERE k > '';
	SELECT count(*), sum(length(k)) FROM rdo WHERE k > '';
}
step s1_o_bmp
{
	SET LOCAL enable_indexscan = off;
	SET LOCAL enable_bitmapscan = on;
	EXPLAIN (COSTS OFF) SELECT count(*), sum(id) FROM rdo WHERE k > '';
	SELECT count(*), sum(id) FROM rdo WHERE k > '';
}
step s1_o_seq
{
	SET LOCAL enable_seqscan = on;
	SET LOCAL enable_bitmapscan = off;
	EXPLAIN (COSTS OFF) SELECT count(*), sum(id), sum(length(k)) FROM rdo WHERE k > '';
	SELECT count(*), sum(id), sum(length(k)) FROM rdo WHERE k > '';
}
step s1_m
{
	SELECT count(*) AS missing FROM
		(SELECT DISTINCT e, rdm.ctid FROM rdm, unnest(a) e
		 EXCEPT SELECT key, tid FROM bark_multikey_entries('rdm_a')) x;
}
step s1_commit	{ COMMIT; }
step s1_after
{
	BEGIN;
	SET LOCAL enable_seqscan = off;
	SET LOCAL enable_bitmapscan = off;
	EXPLAIN (COSTS OFF) SELECT count(*), sum(k) FROM rd WHERE id BETWEEN 1 AND 20;
	SELECT count(*), sum(k) FROM rd WHERE id BETWEEN 1 AND 20;
	SET LOCAL enable_seqscan = on;
	SET LOCAL enable_indexscan = off;
	SELECT count(*), sum(k) FROM rd WHERE id BETWEEN 1 AND 20;
	COMMIT;
}

session s2
step s2_pre		{ CREATE INDEX rd_i ON rd USING bark (id); }
step s2_change
{
	DELETE FROM rd WHERE id IN (5, 10000);
	INSERT INTO rd VALUES (5, 1005, 0, 'again');
	UPDATE rd SET k = k + 1000, h = h + 1 WHERE id = 7;
	UPDATE rd SET id = -id, h = h + 1 WHERE id = 9;
}
step s2_build	{ CREATE INDEX rd_i ON rd USING bark (id); }
step s2_build_unique	{ CREATE UNIQUE INDEX rd_i ON rd USING bark (id); }
step s2_dup		{ INSERT INTO rd VALUES (7, 0, 0, 'dup'); }
step s2_build_par
{
	SET min_parallel_table_scan_size = 0;
	SET max_parallel_maintenance_workers = 2;
	CREATE INDEX rd_i ON rd USING bark (id);
	RESET min_parallel_table_scan_size;
	RESET max_parallel_maintenance_workers;
}
step s2_build_par_unique
{
	SET min_parallel_table_scan_size = 0;
	SET max_parallel_maintenance_workers = 2;
	CREATE UNIQUE INDEX rd_i ON rd USING bark (id);
	RESET min_parallel_table_scan_size;
	RESET max_parallel_maintenance_workers;
}
step s2_reindex	{ REINDEX INDEX rd_i; }
step s2_reindex_conc	{ REINDEX INDEX CONCURRENTLY rd_i; }
step s2_change_m
{
	DELETE FROM rdm WHERE id = 3;
	UPDATE rdm SET a = ARRAY[9999], h = h + 1 WHERE id = 4;
}
step s2_build_m	{ CREATE INDEX rdm_a ON rdm USING bark (a bark_int4_array_ops); }
step s2_change_o
{
	DELETE FROM rdo WHERE id IN (2, 102);
	INSERT INTO rdo VALUES (12, rd_big(2)), (112, 'small2');
}
step s2_build_o	{ CREATE UNIQUE INDEX rdo_k ON rdo USING bark (k); }
step s2_check
{
	SELECT c.relname, bark_index_parent_check(c.oid, true)
	FROM pg_class c JOIN pg_am a ON a.oid = c.relam
	WHERE a.amname = 'bark' AND c.relname IN ('rd_i', 'rdo_k');
}

permutation s1_begin s2_change s2_build s1_idx s1_ios s1_bmp s1_seq s1_commit s2_check
permutation s1_begin s2_change s2_build_unique s2_dup s1_idx s1_ios s1_bmp s1_seq s1_commit s2_check
permutation s1_begin s2_change_m s2_build_m s1_m s1_commit
permutation s1_begin s2_change_o s2_build_o s1_o_idx s1_o_ios s1_o_bmp s1_o_seq s1_commit s2_check
permutation s1_begin s2_change s2_build_par s1_idx s1_ios s1_bmp s1_seq s1_commit s2_check
permutation s1_begin s2_change s2_build_par_unique s1_idx s1_ios s1_bmp s1_seq s1_commit s2_check
permutation s2_pre s1_begin s2_change s2_reindex s1_idx s1_ios s1_bmp s1_seq s1_commit s2_check
permutation s2_pre s1_begin s2_change s2_reindex_conc s1_idx s1_ios s1_bmp s1_seq s1_commit s1_after s2_check
