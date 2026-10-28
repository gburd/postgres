# HOT updates and exclusion constraints, concurrently.
#
# An interval hashes as hashint8() of its span in microseconds, which folds
# the high half into the low half, so spans of 1 and 2^32 microseconds have
# the same hash code: a hash index gives them the same entry, and changing a
# row's k from one to the other can be a HOT update.  (Not int8: its
# equality implies image equality, so the index is not asked.)  The exclusion
# constraint must still be checked against the new value, and a concurrent
# insertion of the same value must still see the update, and the update
# the insertion, whichever comes first.

setup
{
	CREATE TABLE hot_excl (id int, k interval, EXCLUDE USING hash (k WITH =))
		WITH (fillfactor = 10);
	INSERT INTO hot_excl VALUES (1, '00:00:00.000001');
}

teardown
{
	DROP TABLE hot_excl;
}

session s1
step s1_begin	{ BEGIN; }
step s1_upd		{ UPDATE hot_excl SET k = '01:11:34.967296' WHERE id = 1; }
step s1_hot		{ SELECT pg_stat_get_xact_tuples_hot_updated('hot_excl'::regclass) AS hot; }
step s1_commit	{ COMMIT; }
step s1_abort	{ ROLLBACK; }

session s2
step s2_begin	{ BEGIN; }
step s2_ins		{ INSERT INTO hot_excl VALUES (2, '01:11:34.967296'); }
step s2_ins_old	{ INSERT INTO hot_excl VALUES (3, '00:00:00.000001'); }
step s2_commit	{ COMMIT; }
step s2_abort	{ ROLLBACK; }
step s2_check
{
	SET enable_seqscan = off;
	SET enable_bitmapscan = off;
	SELECT id, k FROM hot_excl WHERE k IN ('00:00:00.000001', '01:11:34.967296') ORDER BY id;
	RESET enable_seqscan;
	RESET enable_bitmapscan;
}

# the HOT update comes first: the insertion waits for it, then fails
permutation s1_begin s1_upd s1_hot s2_ins s1_commit s2_check
# ... or succeeds if the update rolls back
permutation s1_begin s1_upd s2_ins s1_abort s2_check
# the insertion comes first: the HOT update waits for it, then fails
permutation s2_begin s2_ins s1_upd s2_commit s2_check
# ... or succeeds, and is HOT, if the insertion rolls back
permutation s2_begin s2_ins s1_begin s1_upd s2_abort s1_hot s1_commit s2_check
# the old value, after the HOT update: the insertion finds the old version
# through the same entries, waits, and succeeds once the update commits ...
permutation s1_begin s1_upd s2_ins_old s1_commit s2_check
# ... or fails if the update rolls back
permutation s1_begin s1_upd s2_ins_old s1_abort s2_check
