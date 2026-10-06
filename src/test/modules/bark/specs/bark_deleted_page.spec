# Deleted BARK pages keep their links, and are reused only when no
# transaction can still hold a link to them.
#
# The setup builds an index on 3000 keys (leaf blocks 1 and 3-10 in key
# order, root 2) and deletes the keys 1000-2990, which empties leaves 5-9.
# VACUUM then deletes those five leaves from the tree.
#
# The first permutation stops an index scan's descent after it has read the
# root's downlink to a leaf that VACUUM is about to delete
# (bark-search-descend).  VACUUM deletes the leaf, and the descent then reads
# a deleted page.  The page kept its right link, so the descent moves right
# to the next live leaf and the scan returns the right rows; a deleted page
# with no right link ends the descent with "fell off the end".
#
# The second permutation holds a REPEATABLE READ snapshot taken before the
# deletion.  Pages deleted under it stay out of the free space map, through a
# second VACUUM that runs after a later transaction has ended, so a leaf
# split extends the index by one page.  Once the snapshot is gone, the
# next VACUUM puts the five pages in the free space map and the next split
# reuses one: the index does not grow.
#
# The third permutation positions a cursor on leaf 4, the left neighbour of
# the deleted leaves, while VACUUM is stopped at the end of its leaf pass
# (bark-bulkdelete-after-page on the last block), so that the cursor's pin
# does not hold up the leaf pass.  VACUUM then deletes the leaves, and FETCH
# ALL must return exactly the live rows after the cursor position, once each
# and in order.  The scan here reads leaf 4's right link only when it leaves
# that page, after the deletion; a scan that reads it when it reads the page,
# as nbtree's does, steps onto the deleted pages and relies on their links.

setup
{
	CREATE EXTENSION injection_points;
	CREATE EXTENSION amcheck;
	CREATE EXTENSION pg_freespacemap;
	CREATE TABLE bark_dp (a int4) WITH (autovacuum_enabled = off);
	INSERT INTO bark_dp SELECT g FROM generate_series(1, 3000) g;
	CREATE INDEX bark_dp_idx ON bark_dp USING bark (a);
	CREATE TABLE bark_dp_built WITH (autovacuum_enabled = off) AS
		SELECT pg_relation_size('bark_dp_idx') / 8192 AS built;
	DELETE FROM bark_dp WHERE a BETWEEN 1000 AND 2990;

	-- Fetch the rest of cursor `cur` and summarize what came back.
	CREATE FUNCTION bark_dp_fetch_rest(cur refcursor) RETURNS text
	LANGUAGE plpgsql AS $$
	DECLARE
		v int;
		prev int;
		lo int;
		n int := 0;
		disorder int := 0;
	BEGIN
		LOOP
			FETCH cur INTO v;
			EXIT WHEN NOT FOUND;
			n := n + 1;
			IF prev IS NOT NULL AND v <= prev THEN
				disorder := disorder + 1;
			END IF;
			lo := coalesce(lo, v);
			prev := v;
		END LOOP;
		RETURN format('%s rows from %s to %s, %s repeated or out of order',
					  n, lo, prev, disorder);
	END $$;
}

teardown
{
	DROP TABLE bark_dp, bark_dp_built;
	DROP FUNCTION bark_dp_fetch_rest(refcursor);
	DROP EXTENSION pg_freespacemap;
	DROP EXTENSION amcheck;
	DROP EXTENSION injection_points;
}

session s1
setup
{
	SELECT injection_points_set_local();
	SET enable_seqscan = off;
	SET enable_bitmapscan = off;
}
step s1_attach_descend
{
	SELECT injection_points_attach('bark-search-descend', 'wait');
}
step s1_count	{ SELECT count(*) AS index_count FROM bark_dp WHERE a >= 1500; }
step s1_snapshot
{
	BEGIN ISOLATION LEVEL REPEATABLE READ;
	SELECT count(*) AS snapshot_count FROM bark_dp;
}
step s1_commit	{ COMMIT; }
step s1_cursor
{
	BEGIN;
	DECLARE c CURSOR FOR SELECT a FROM bark_dp WHERE a > 0 ORDER BY a;
	MOVE FORWARD 800 IN c;
}
step s1_fetch_rest
{
	SELECT bark_dp_fetch_rest('c');
	COMMIT;
}
step s1_check
{
	SELECT bark_index_check('bark_dp_idx');
	BEGIN;
	SET LOCAL enable_seqscan = on;
	SET LOCAL enable_indexscan = off;
	SET LOCAL enable_indexonlyscan = off;
	SELECT count(*) AS heap_count_from_1500 FROM bark_dp WHERE a >= 1500;
	SELECT count(*) AS heap_count FROM bark_dp;
	COMMIT;
}

session s2
setup
{
	SELECT injection_points_set_local();
}
step s2_attach_after
{
	SELECT injection_points_attach('bark-bulkdelete-after-page', 'wait',
		(pg_relation_size('bark_dp_idx') / 8192 - 1)::text);
}
step s2_vacuum	{ VACUUM (INDEX_CLEANUP ON, TRUNCATE OFF) bark_dp; }
step s2_insert1	{ INSERT INTO bark_dp SELECT g FROM generate_series(3001, 3500) g; }
step s2_insert2	{ INSERT INTO bark_dp SELECT g FROM generate_series(3501, 3800) g; }
step s2_xid		{ SELECT pg_current_xact_id() IS NOT NULL AS xid_assigned; }
step s2_report
{
	SELECT pg_relation_size('bark_dp_idx') / 8192 - built AS pages_added,
		   (SELECT count(*) FROM pg_freespace('bark_dp_idx')
			 WHERE avail > 0) AS free_pages
	  FROM bark_dp_built;
}

session s3
step s3_wakeup_descend
{
	SELECT injection_points_detach('bark-search-descend');
	SELECT injection_points_wakeup('bark-search-descend');
}
step s3_wakeup_after
{
	SELECT injection_points_detach('bark-bulkdelete-after-page');
	SELECT injection_points_wakeup('bark-bulkdelete-after-page');
}

# Each step that follows a wakeup runs in the woken session, so that it waits
# for the woken step to finish.
permutation s1_attach_descend s1_count s2_vacuum s3_wakeup_descend s1_check

permutation s1_snapshot s2_vacuum s2_xid s2_vacuum s2_report s2_insert1
	s2_report s1_commit s2_vacuum s2_report s2_insert2 s2_report s1_check

# The final free_pages shows that the first VACUUM, which finished before the
# FETCH, deleted the five leaves: pages deleted by the second VACUUM could not
# be recycled yet when it ends, and no free page would be reported.
permutation s2_attach_after s2_vacuum s1_cursor s3_wakeup_after s2_report
	s1_fetch_rest s2_xid s2_vacuum s2_report s1_check
