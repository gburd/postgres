# VACUUM of a BARK index running concurrently with a leaf split, and with a
# scan that holds a pin on a leaf.
#
# barkbulkdelete scans the index in physical block order.  A leaf split that
# happens while it runs can move entries from a leaf it has not reached yet to
# a new right page at a block it has already passed, when the free space map
# hands out a low block.  The split stamps both halves with the VACUUM's cycle
# ID, and when VACUUM reaches the left half it follows the right link back to
# the low block.  Without that, the dead entries moved to the low block survive
# VACUUM while the heap line pointers they reference are freed.
#
# The setup builds an index, deletes and vacuums a key range so that leaf
# blocks are deleted and recorded in the free space map (bark_vs_free), then
# deletes every other row of a higher key range for the next VACUUM to remove.
#
# The first permutation stops VACUUM after the block just above the free ones
# (bark-bulkdelete-after-page, conditioned on that block number), then splits
# a leaf in the high key range: the new right page reuses a free block, below
# the scan position.  After VACUUM finishes, the heap pages it cleaned are
# all-visible, so an index-only scan counts any entry left behind without
# visiting the heap; the count must equal the heap's.
#
# The second permutation checks that VACUUM takes a cleanup lock on each leaf:
# a cursor holds a pin on the first leaf, and VACUUM must wait for the pin to
# be released (wait event BufferCleanup) rather than delete entries from the
# page under the scan.  The index-only scan keeps no heap page pinned, since
# the heap page it reads is all-visible, so the only pin VACUUM can wait for
# is the one on the leaf.

setup
{
	CREATE EXTENSION injection_points;
	CREATE EXTENSION amcheck;
	CREATE EXTENSION pg_freespacemap;
	CREATE TABLE bark_vs (a int4) WITH (autovacuum_enabled = off);
	INSERT INTO bark_vs SELECT 10 * g FROM generate_series(1, 3000) g;
	CREATE INDEX bark_vs_idx ON bark_vs USING bark (a);
	DELETE FROM bark_vs WHERE a BETWEEN 3010 AND 12000;
}
setup
{
	VACUUM (INDEX_CLEANUP ON) bark_vs;
}
setup
{
	CREATE TABLE bark_vs_free AS
		SELECT blkno FROM pg_freespace('bark_vs_idx') WHERE avail > 0;
	DELETE FROM bark_vs WHERE a BETWEEN 20010 AND 30000 AND a % 20 = 0;

	-- Wait until session `sess` is either waiting for a cleanup lock or done
	-- with its VACUUM, and say which.
	CREATE FUNCTION bark_wait_for_cleanup_lock(sess text) RETURNS text
	LANGUAGE plpgsql AS $$
	DECLARE
		r text;
	BEGIN
		LOOP
			PERFORM pg_stat_clear_snapshot();
			SELECT CASE WHEN wait_event = 'BufferCleanup' THEN 'waiting for cleanup lock'
						WHEN state = 'idle' THEN 'not waiting' END
			  INTO r
			  FROM pg_stat_activity
			 WHERE application_name LIKE '%/' || sess AND query LIKE 'VACUUM%';
			IF r IS NOT NULL THEN
				RETURN r;
			END IF;
			PERFORM pg_sleep(0.01);
		END LOOP;
	END $$;
}

teardown
{
	DROP TABLE bark_vs, bark_vs_free;
	DROP FUNCTION bark_wait_for_cleanup_lock(text);
	DROP EXTENSION pg_freespacemap;
	DROP EXTENSION amcheck;
	DROP EXTENSION injection_points;
}

session s1
setup
{
	SELECT injection_points_set_local();
}
step s1_attach
{
	SELECT injection_points_attach('bark-bulkdelete-after-page', 'wait',
		(SELECT max(blkno) + 1 FROM bark_vs_free)::text);
}
step s1_vacuum	{ VACUUM (INDEX_CLEANUP ON) bark_vs; }
step s1_check
{
	SELECT bark_index_check('bark_vs_idx');
	SET enable_seqscan = off;
	SET enable_bitmapscan = off;
	EXPLAIN (COSTS OFF) SELECT count(*) FROM bark_vs WHERE a > 0;
	SELECT count(*) AS index_only_count FROM bark_vs WHERE a > 0;
	RESET enable_seqscan;
	RESET enable_bitmapscan;
	SELECT count(*) AS heap_count FROM bark_vs;
}

session s2
setup
{
	SET enable_seqscan = off;
	SET enable_bitmapscan = off;
}
step s2_split
{
	INSERT INTO bark_vs SELECT 10 * g + 5 FROM generate_series(2300, 2500) g;
	SELECT count(*) > 0 AS split_reused_free_block
	  FROM bark_vs_free f
	 WHERE pg_freespace('bark_vs_idx', f.blkno) = 0;
}
step s2_pin
{
	BEGIN;
	DECLARE c CURSOR FOR SELECT a FROM bark_vs WHERE a > 0 ORDER BY a;
	FETCH 1 FROM c;
}
step s2_unpin
{
	SELECT bark_wait_for_cleanup_lock('s1');
	COMMIT;
}

session s3
step s3_wakeup
{
	SELECT injection_points_detach('bark-bulkdelete-after-page');
	SELECT injection_points_wakeup('bark-bulkdelete-after-page');
}

permutation s1_attach s1_vacuum s2_split s3_wakeup s1_check

# isolationtester does not see a wait for a cleanup lock, hence (*); and s2
# both waits for it and releases the pin, in one step, because the tester
# would otherwise poll the blocked VACUUM until it timed out.
permutation s2_pin s1_vacuum(*) s2_unpin s1_check
