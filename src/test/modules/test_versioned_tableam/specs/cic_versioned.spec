# CREATE INDEX CONCURRENTLY on heap_versioned, with concurrent updates.
#
# The build waits for an advisory lock that s1 holds, inside the index
# expression, so s1's updates run while the index is not yet ready, and
# again (s1_upd2) once it is ready but not yet valid.  Validation hands
# heap_versioned (TID, tag) pairs, and heap_versioned checks each one.
#
# The second permutation makes the build fail with a unique violation in
# validation, leaving an index that is ready but not valid.  Updates still
# make tagged entries in it, and the executor still lets heap_versioned keep
# a row's locator across a change to its column (keep_locator = pretend
# reports so), since a ready index is maintained.  The invalid index still
# enforces uniqueness, so the new values are unique.

setup
{
	CREATE EXTENSION test_versioned_tableam;
	CREATE TABLE cv (id int PRIMARY KEY, k int, s text) USING heap_versioned;
	INSERT INTO cv SELECT g, g, 'abc' || g FROM generate_series(1, 50) g;
	CREATE FUNCTION cv_lock(int) RETURNS int IMMUTABLE LANGUAGE plpgsql AS $$
		BEGIN PERFORM pg_advisory_lock_shared(4243); PERFORM pg_advisory_unlock_shared(4243); RETURN $1; END;
	$$;
}

teardown
{
	DROP TABLE cv;
	DROP FUNCTION cv_lock(int);
	DROP EXTENSION test_versioned_tableam;
}

session s1
step s1_lock	{ SELECT pg_advisory_lock(4243); }
step s1_upd		{ UPDATE cv SET k = k + 1000 WHERE id <= 10; }
step s1_unlock	{ SELECT pg_advisory_unlock(4243); }
step s1_upd2	{ UPDATE cv SET k = k + 1000 WHERE id > 40; }
step s1_dup		{ UPDATE cv SET k = 1 WHERE id = 2; }
step s1_check
{
	SET enable_seqscan = off;
	SET enable_bitmapscan = off;
	SELECT count(*) FROM cv WHERE cv_lock(k) > 0;
	SELECT count(*) FROM cv WHERE cv_lock(k) = 1005;
	RESET enable_seqscan;
	RESET enable_bitmapscan;
	SELECT indisready, indisvalid FROM pg_index WHERE indexrelid = 'cv_k'::regclass;
	SELECT counter FROM test_versioned_seen
	 WHERE counter IN ('validate_tagged', 'insert_tagged', 'scan_tagged');
}
step s1_reset	{ SELECT test_versioned_reset(); }
step s1_ready
{
	SELECT indisready, indisvalid FROM pg_index WHERE indexrelid = 'cv_k'::regclass;
	SELECT test_versioned_reset();
	UPDATE cv SET k = 5000 WHERE id = 30;
	SELECT test_versioned_tag('cv_k', ctid) = xmin::text::bigint AS tagged
	  FROM cv WHERE id = 30;
	SET test_versioned_tableam.keep_locator = pretend;
	UPDATE cv SET k = 5001 WHERE id = 31;
	RESET test_versioned_tableam.keep_locator;
	SELECT counter FROM test_versioned_seen
	 WHERE counter IN ('insert_tagged', 'update_kept');
}

session s2
step s2_cic		{ CREATE INDEX CONCURRENTLY cv_k ON cv (cv_lock(k)); }
step s2_cic_u	{ CREATE UNIQUE INDEX CONCURRENTLY cv_k ON cv (cv_lock(k)); }

permutation s1_reset s1_lock s2_cic s1_upd s1_unlock s1_upd2 s1_check
permutation s1_lock s2_cic_u s1_dup s1_unlock s1_ready
