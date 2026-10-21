# HOT updates and a BARK exclusion constraint, concurrently.
#
# barkcomparedatums compares a scalar column by image, so an UPDATE that
# writes a value stored compressed back in plain form (t || '') keeps the
# index's entries and is HOT; the executor then checks the constraint itself
# (ExecCheckHotExclusion).  A concurrent insertion of the same value finds
# the old entries, which lead to both versions of the row, so it waits for
# the update and fails whichever way the update ends; once the update has
# committed and VACUUM has pruned the old version, the entries still lead
# to the new one.  The new value is the old one, so a row that conflicts
# with it conflicts with the row before the update too: unlike
# hot-exclusion's hash index, no insertion can come first and make the
# update wait.

setup
{
	CREATE TABLE bark_xh (id int, t text, EXCLUDE USING bark (t WITH =))
		WITH (fillfactor = 10, autovacuum_enabled = off);
	INSERT INTO bark_xh VALUES (1, repeat('e', 3000));
}

teardown
{
	DROP TABLE bark_xh;
}

session s1
step s1_begin	{ BEGIN; }
step s1_upd		{ UPDATE bark_xh SET t = t || '' WHERE id = 1; }
step s1_hot		{ SELECT pg_stat_get_xact_tuples_hot_updated('bark_xh'::regclass) AS hot; }
step s1_commit	{ COMMIT; }
step s1_abort	{ ROLLBACK; }
step s1_vacuum	{ VACUUM bark_xh; }

session s2
step s2_ins		{ INSERT INTO bark_xh VALUES (2, repeat('e', 3000)); }
step s2_check
{
	SET enable_seqscan = off;
	SET enable_bitmapscan = off;
	SELECT id, length(t) FROM bark_xh WHERE t = repeat('e', 3000) ORDER BY id;
	RESET enable_seqscan;
	RESET enable_bitmapscan;
}

# the HOT update comes first: the insertion waits for it, then fails
permutation s1_begin s1_upd s1_hot s2_ins s1_commit s2_check
# ... and fails too if the update rolls back, against the old version
permutation s1_begin s1_upd s2_ins s1_abort s2_check
# after the HOT update and pruning, the old entries lead to the new version
permutation s1_begin s1_upd s1_hot s1_commit s1_vacuum s2_ins s2_check
