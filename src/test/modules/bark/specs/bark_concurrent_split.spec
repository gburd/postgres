# Concurrent inserts across a BARK page split.
#
# A split writes the new right page and right link in one WAL record and the
# right page's downlink into the parent in a second, with the left page
# flagged BARK_INCOMPLETE_SPLIT in between.  The splitting backend must keep
# the left page locked until the parent insert is done; otherwise a second
# inserter can see the flag, "finish" the split by inserting the same
# downlink, and leave the first backend to insert it again and clear a flag
# that is already clear.
#
# s1 stops at bark-leave-leaf-split-incomplete in the middle of a leaf split.
# s2 then inserts into the same leaf.  s2 must wait for s1's lock on the left
# page instead of finishing s1's split: s3 checks that s2 is waiting on a
# buffer lock before waking s1.  bark-finish-incomplete-split is attached in
# notice mode in s2, so a NOTICE in the output would mean s2 inserted a
# downlink for s1's split; and s1 clearing the flag asserts that it was still
# set.  Together those show the split got exactly one downlink.  Finally the
# index is checked with bark_index_check and against a heap count.
#
# The first permutation splits the root leaf of a one-page index (a new root
# is created); the second splits the rightmost leaf of a multi-level index
# (the downlink goes into an existing parent).

setup
{
	CREATE EXTENSION injection_points;
	CREATE EXTENSION amcheck;
	CREATE TABLE bark_split_root (i int4) WITH (autovacuum_enabled = off);
	CREATE INDEX bark_split_root_idx ON bark_split_root USING bark (i);
	INSERT INTO bark_split_root SELECT g FROM generate_series(1, 100) g;
	CREATE TABLE bark_split_leaf (i int4) WITH (autovacuum_enabled = off);
	CREATE INDEX bark_split_leaf_idx ON bark_split_leaf USING bark (i);
	INSERT INTO bark_split_leaf SELECT g FROM generate_series(1, 5000) g;

	-- Wait until session `sess` is either blocked on a buffer lock or done
	-- with its INSERT, and say which.
	CREATE FUNCTION bark_wait_for_buffer_lock(sess text) RETURNS text
	LANGUAGE plpgsql AS $$
	DECLARE
		r text;
	BEGIN
		LOOP
			PERFORM pg_stat_clear_snapshot();
			SELECT CASE WHEN wait_event_type = 'Buffer' THEN 'blocked on buffer lock'
						WHEN state = 'idle' THEN 'not blocked' END
			  INTO r
			  FROM pg_stat_activity
			 WHERE application_name LIKE '%/' || sess AND query LIKE 'INSERT INTO%';
			IF r IS NOT NULL THEN
				RETURN r;
			END IF;
			PERFORM pg_sleep(0.01);
		END LOOP;
	END $$;
}

teardown
{
	DROP TABLE bark_split_root;
	DROP TABLE bark_split_leaf;
	DROP FUNCTION bark_wait_for_buffer_lock(text);
	DROP EXTENSION amcheck;
	DROP EXTENSION injection_points;
}

session s1
setup
{
	SELECT injection_points_set_local();
	SELECT injection_points_attach('bark-leave-leaf-split-incomplete', 'wait');
}
step s1_insert_root	{ INSERT INTO bark_split_root SELECT g FROM generate_series(101, 1100) g; }
step s1_insert_leaf	{ INSERT INTO bark_split_leaf SELECT g FROM generate_series(5001, 6000) g; }

session s2
setup
{
	SELECT injection_points_set_local();
	SELECT injection_points_attach('bark-finish-incomplete-split', 'notice');
}
step s2_insert_root	{ INSERT INTO bark_split_root VALUES (0); }
step s2_insert_leaf	{ INSERT INTO bark_split_leaf VALUES (1000000); }
step s2_check_root
{
	SELECT bark_index_check('bark_split_root_idx');
	SET enable_seqscan = off;
	SET enable_bitmapscan = off;
	SELECT count(*) AS index_count FROM bark_split_root WHERE i >= 0;
	RESET enable_seqscan;
	RESET enable_bitmapscan;
	SELECT count(*) AS heap_count FROM bark_split_root;
}
step s2_check_leaf
{
	SELECT bark_index_check('bark_split_leaf_idx');
	SET enable_seqscan = off;
	SET enable_bitmapscan = off;
	SELECT count(*) AS index_count FROM bark_split_leaf WHERE i >= 0;
	RESET enable_seqscan;
	RESET enable_bitmapscan;
	SELECT count(*) AS heap_count FROM bark_split_leaf;
}
teardown
{
	SELECT injection_points_detach('bark-finish-incomplete-split');
}

session s3
step s3_release
{
	SELECT bark_wait_for_buffer_lock('s2');
	SELECT injection_points_detach('bark-leave-leaf-split-incomplete');
	SELECT injection_points_wakeup('bark-leave-leaf-split-incomplete');
}

# The buffer-lock wait in s2 is invisible to isolationtester, hence (*); the
# s1 marker keeps the completion order fixed.
permutation s1_insert_root s2_insert_root(*, s1_insert_root) s3_release s2_check_root
permutation s1_insert_leaf s2_insert_leaf(*, s1_insert_leaf) s3_release s2_check_leaf
