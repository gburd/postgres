# INSERT ... ON CONFLICT on a UNIQUE ... NULLS NOT DISTINCT BARK index,
# racing a plain insert of the same NULL key.
#
# s1's arbiter pre-check finds no row with a NULL key and stops at
# check-exclusion-or-unique-constraint-no-conflict, before its speculative
# insertion.  s2 then inserts a NULL key and commits.  When s1 resumes, only
# the index's own check at insert time (UNIQUE_CHECK_PARTIAL) can see s2's
# row: it must report the conflict, so that s1 kills its tuple, checks again
# and does nothing, or updates s2's row.  One row with a NULL key remains.

setup
{
	CREATE EXTENSION injection_points;
	CREATE TABLE bark_nnd (a int, v text);
	CREATE UNIQUE INDEX bark_nnd_idx ON bark_nnd USING bark (a)
		NULLS NOT DISTINCT;
}

teardown
{
	DROP TABLE bark_nnd;
	DROP EXTENSION injection_points;
}

session s1
setup
{
	SELECT injection_points_set_local();
	SELECT injection_points_attach('check-exclusion-or-unique-constraint-no-conflict', 'wait');
}
step s1_nothing
{
	INSERT INTO bark_nnd VALUES (NULL, 's1') ON CONFLICT (a) DO NOTHING;
}
step s1_update
{
	INSERT INTO bark_nnd VALUES (NULL, 's1') ON CONFLICT (a)
		DO UPDATE SET v = 's1 updated ' || bark_nnd.v;
}
step s1_select	{ SELECT a, v FROM bark_nnd ORDER BY v; }

session s2
step s2_insert	{ INSERT INTO bark_nnd VALUES (NULL, 's2'); }
step s2_wakeup
{
	SELECT injection_points_detach('check-exclusion-or-unique-constraint-no-conflict');
	SELECT injection_points_wakeup('check-exclusion-or-unique-constraint-no-conflict');
}

permutation s1_nothing s2_insert s2_wakeup s1_select
permutation s1_update s2_insert s2_wakeup s1_select
