# An UPDATE of a multikey BARK index's column that waits on a concurrent
# UPDATE changing the column's keys.  After the wait the waiting UPDATE
# re-fetches the row, recomputes its new value from the version the other
# wrote, and must compare the keys with that version's, not with those of
# the version it first read.  s1_rev reverses every row's array, which
# keeps each key set, so both rows are HOT, the waiting one included (its
# new value has the key 99 the other UPDATE added, and the index has an
# entry of it for that version).  s1_const sets the waiting row back to
# the first version's keys: compared with the version it waited for, that
# drops the key 99, so it is not HOT, and no scan may find the row by 99.
# The waiting row is the second row of s1_rev.

setup
{
	CREATE EXTENSION IF NOT EXISTS bark_multikey;
	CREATE EXTENSION IF NOT EXISTS amcheck;
	CREATE TABLE mhe (id int, a int4[]) WITH (fillfactor = 50);
	CREATE INDEX mhe_a ON mhe USING bark (a bark_int4_array_ops);
	INSERT INTO mhe VALUES (1, ARRAY[10,11]), (2, ARRAY[20,21]);
}

teardown
{
	DROP TABLE mhe;
}

session s1
step s1_begin	{ BEGIN; }
step s1_rev		{ UPDATE mhe SET a = array_reverse(a); }
step s1_const	{ UPDATE mhe SET a = ARRAY[21,20] WHERE id = 2; }
step s1_count
{
	SELECT pg_stat_get_xact_tuples_updated('mhe'::regclass) AS upd,
		   pg_stat_get_xact_tuples_hot_updated('mhe'::regclass) AS hot;
}
step s1_commit	{ COMMIT; }
step s1_check
{
	SET enable_seqscan = off;
	SET enable_bitmapscan = off;
	SELECT id, a FROM mhe WHERE a && ARRAY[99];
	SELECT id, a FROM mhe WHERE a && ARRAY[11,20] ORDER BY id;
	SET enable_indexscan = off;
	SET enable_bitmapscan = on;
	SELECT id, a FROM mhe WHERE a && ARRAY[99];
	SELECT id, a FROM mhe WHERE a && ARRAY[11,20] ORDER BY id;
	RESET enable_seqscan;
	RESET enable_indexscan;
	RESET enable_bitmapscan;
	SELECT id, a FROM mhe ORDER BY id;
	SELECT bark_index_check('mhe_a', true);
}

session s2
step s2_begin	{ BEGIN; }
step s2_add		{ UPDATE mhe SET a = a || 99 WHERE id = 2; }
step s2_commit	{ COMMIT; }

permutation s2_begin s2_add s1_begin s1_rev s2_commit s1_count s1_commit s1_check
permutation s2_begin s2_add s1_begin s1_const s2_commit s1_count s1_commit s1_check
