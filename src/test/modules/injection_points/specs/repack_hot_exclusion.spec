# REPACK (CONCURRENTLY) replaying an UPDATE that kept an exclusion
# constraint's index entries.
#
# An interval's hash code is hashint8() of its span in microseconds, so
# spans of 1 and 2^32 microseconds collide, and changing a row's k from one
# to the other keeps its hash index entry (a HOT update).  When REPACK
# replays that update on the new table, the index there must find the row
# under its new value, and the constraint must still hold.
setup
{
	CREATE EXTENSION injection_points;

	CREATE TABLE repack_hot_excl (id int PRIMARY KEY, k interval,
		EXCLUDE USING hash (k WITH =)) WITH (fillfactor = 10);
	INSERT INTO repack_hot_excl VALUES (1, '00:00:00.000001'), (2, '00:00:00.000002');
}

teardown
{
	DROP TABLE repack_hot_excl;
	DROP EXTENSION injection_points;
}

session s1
setup
{
	SELECT injection_points_set_local();
	SELECT injection_points_attach('repack-concurrently-before-lock', 'wait');
}
step wait_before_lock
{
	REPACK (CONCURRENTLY) repack_hot_excl USING INDEX repack_hot_excl_pkey;
}
step check_after_repack
{
	SET enable_seqscan = off;
	SET enable_bitmapscan = off;
	SELECT id, k FROM repack_hot_excl WHERE k = '01:11:34.967296';
	SELECT id, k FROM repack_hot_excl WHERE k = '00:00:00.000001';
	RESET enable_seqscan;
	RESET enable_bitmapscan;
}
step insert_conflict
{
	INSERT INTO repack_hot_excl VALUES (3, '01:11:34.967296');
}
teardown
{
	SELECT injection_points_detach('repack-concurrently-before-lock');
}

session s2
step update_hot
{
	UPDATE repack_hot_excl SET k = '01:11:34.967296' WHERE id = 1;
	SELECT pg_stat_get_xact_tuples_hot_updated('repack_hot_excl'::regclass)
		AS hot;
}
step wakeup_before_lock
{
	SELECT injection_points_wakeup('repack-concurrently-before-lock');
}

permutation
	wait_before_lock
	update_hot
	wakeup_before_lock
	check_after_repack
	insert_conflict
