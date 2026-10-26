# T12b: a tagged multikey BARK index made an exclusion constraint's while
# an in-place update of its column is in progress (V2-DESIGN 5.2, 12.1).
#
# Row 1 is {1,2}.  s1 updates it in place to {2,3} (keep_locator =
# inplace: heap's HOT chain at the row's locator L holds both versions)
# and stops after its index inserts, at an injection point a statement
# trigger runs: the index then holds (1,L,0) (2,L,0) (2,L,t1) (3,L,t1).
# s3 makes the index an exclusion constraint's on array = while s1 is
# stopped, as DocumentDB's prepareUnique does, with only AccessShareLock on
# the table, so nothing waits for s1.  s2 inserts {2,3}.  Its check scans
# the points 2 and 3 under a dirty snapshot, which sees both of row 1's
# versions.  It must meet v1 = {2,3}, whose insert is in progress, and wait
# for s1; then it fails if s1 commits and succeeds if s1 rolls back.  A
# scan that keeps one row per locator (rule D's R) returns v0 = {1,2}
# through (2,L,0), whose recheck fails, and drops the entries that lead to
# v1, so s2 commits a duplicate.

setup
{
	CREATE EXTENSION test_versioned_tableam;
	CREATE EXTENSION bark_multikey;
	CREATE EXTENSION injection_points;
	CREATE EXTENSION amcheck;
}

setup
{
	CREATE TABLE tmx (id int, doc int4[]) USING heap_versioned
	  WITH (autovacuum_enabled = off, fillfactor = 50);
	INSERT INTO tmx VALUES (1, ARRAY[1,2]), (2, ARRAY[20,21]);
	SELECT test_versioned_reset();
	CREATE INDEX tmx_doc ON tmx USING bark (doc bark_int4_array_ops);
	CREATE FUNCTION tmx_pause() RETURNS trigger LANGUAGE plpgsql AS $$
	BEGIN
		PERFORM injection_points_run('tmx-update-done');
		RETURN NULL;
	END $$;
	CREATE TRIGGER tmx_pause AFTER UPDATE ON tmx
	  FOR EACH STATEMENT EXECUTE FUNCTION tmx_pause();
}

teardown
{
	DROP TABLE tmx;
	DROP FUNCTION tmx_pause();
	DROP EXTENSION amcheck;
	DROP EXTENSION injection_points;
	DROP EXTENSION bark_multikey;
	DROP EXTENSION test_versioned_tableam;
}

session s1
setup
{
	SELECT injection_points_set_local();
	SELECT injection_points_attach('tmx-update-done', 'wait');
	SET test_versioned_tableam.keep_locator = inplace;
}
step s1_begin		{ BEGIN; }
step s1_update		{ UPDATE tmx SET doc = ARRAY[2,3] WHERE id = 1; }
step s1_commit		{ COMMIT; }
step s1_rollback	{ ROLLBACK; }
teardown	{ SELECT injection_points_detach('tmx-update-done'); }

session s2
step s2_insert		{ INSERT INTO tmx VALUES (3, ARRAY[2,3]); }

session s3
step s3_convert
{
	SELECT test_versioned_add_exclusion('tmx_doc', '=(anyarray,anyarray)');
	UPDATE pg_catalog.pg_index SET indisexclusion = true
	  WHERE indexrelid = 'tmx_doc'::regclass;
}
step s3_wakeup		{ SELECT injection_points_wakeup('tmx-update-done'); }
step s3_check
{
	SELECT id, doc FROM tmx ORDER BY id;
	SELECT n AS kept FROM test_versioned_counts() WHERE counter = 'update_kept';
	SELECT bark_index_check('tmx_doc');
}

permutation s1_begin s1_update s3_convert s2_insert s3_wakeup s1_commit s3_check
permutation s1_begin s1_update s3_convert s2_insert s3_wakeup s1_rollback s3_check
