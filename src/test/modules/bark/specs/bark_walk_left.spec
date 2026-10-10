# Backward scans that step left while other sessions split or delete pages.
#
# A backward scan cannot trust the left link it saved when it read a page: by
# the time it follows the link, the page to its left may have split, or the
# page it read may have been deleted.  bark_lock_and_validate_left checks
# that the left page's right link still points back at the page just read,
# and recovers when it does not: it steps right (at most four times) to the
# page that does, and otherwise looks at what became of the page just read.
#
# Each permutation stops the scan between pages at bark-walk-left, changes
# the pages next to it, and wakes it.  The notice-mode injection points show
# which recovery steps ran.  Every scan returns what a sequential scan of the
# same rows returns, and the index passes bark_index_check at the end.
#
# The index is built on the 2001 keys 0, 10, ..., 20000: leaves 1, 3, 4, 5,
# 6 and 7 in key order under root 2, leaf 6 holding 14680-18340 and leaf 7
# (the rightmost) 18350-20000.  The page layout assumes 8kB blocks.

setup
{
	CREATE EXTENSION injection_points;
	CREATE EXTENSION amcheck;
	CREATE TABLE bwl (col int4) WITH (autovacuum_enabled = off);
	INSERT INTO bwl SELECT 10 * i FROM generate_series(0, 2000) i;
	CREATE INDEX bwl_idx ON bwl USING bark (col);

	-- Wait until every dead tuple in the table is removable by VACUUM, as
	-- nbtree's backwards-scan test does.
	CREATE PROCEDURE bwl_wait_prunable() LANGUAGE plpgsql AS $$
	DECLARE
		barrier xid8;
		cutoff xid8;
	BEGIN
		barrier := pg_current_xact_id();
		LOOP
			ROLLBACK;
			cutoff := removable_cutoff('pg_database');
			EXIT WHEN cutoff >= barrier;
			PERFORM pg_sleep(.1);
		END LOOP;
	END $$;
}
setup
{
	VACUUM (FREEZE, DISABLE_PAGE_SKIPPING) bwl;
}

teardown
{
	DROP TABLE bwl;
	DROP PROCEDURE bwl_wait_prunable();
	DROP EXTENSION amcheck;
	DROP EXTENSION injection_points;
}

session s1
setup
{
	SELECT injection_points_set_local();
	SET enable_seqscan = off;
	SET enable_bitmapscan = off;
	SET enable_sort = off;
}
step s1_attach
{
	SELECT injection_points_attach('bark-walk-left', 'wait');
	SELECT injection_points_attach('bark-walk-left-step-right', 'notice');
	SELECT injection_points_attach('bark-walk-left-deleted', 'notice');
	SELECT injection_points_attach('bark-walk-left-restart', 'notice');
}
step s1_explain
{
	EXPLAIN (COSTS OFF) SELECT col FROM bwl
	 WHERE col % 1000 = 0 AND pg_backend_pid() <> 0 ORDER BY col DESC;
}
# pg_backend_pid() is parallel restricted, so the scan runs in this backend
# under debug_parallel_query too.
step s1_scan
{
	SELECT array_agg(col) AS index_desc FROM
		(SELECT col FROM bwl WHERE col % 1000 = 0 AND pg_backend_pid() <> 0
		 ORDER BY col DESC) s;
}
step s1_scan_14000
{
	SELECT array_agg(col) AS index_desc FROM
		(SELECT col FROM bwl
		  WHERE col <= 14000 AND col % 1000 = 0 AND pg_backend_pid() <> 0
		  ORDER BY col DESC) s;
}
# The same scan as a plain index scan, which drops its leaf pins.
step s1_scan_plain
{
	SET enable_indexonlyscan = off;
	SELECT array_agg(col) AS index_desc FROM
		(SELECT col FROM bwl WHERE col % 1000 = 0 AND pg_backend_pid() <> 0
		 ORDER BY col DESC) s;
	RESET enable_indexonlyscan;
}
step s1_detach
{
	SELECT injection_points_detach('bark-walk-left-step-right');
	SELECT injection_points_detach('bark-walk-left-deleted');
	SELECT injection_points_detach('bark-walk-left-restart');
}

session s2
step s2_split_once
{
	INSERT INTO bwl SELECT 14681 + 2 * g FROM generate_series(0, 59) g;
}
step s2_split_many
{
	INSERT INTO bwl SELECT g FROM generate_series(14681, 18349) g
	 WHERE g % 10 <> 0;
}
step s2_delete_mid	{ DELETE FROM bwl WHERE col BETWEEN 11010 AND 18340; }
step s2_delete_6	{ DELETE FROM bwl WHERE col BETWEEN 14680 AND 18340; }
step s2_wait_prunable	{ CALL bwl_wait_prunable(); }
step s2_vacuum	{ VACUUM bwl; }
step s2_wakeup
{
	SELECT injection_points_detach('bark-walk-left');
	SELECT injection_points_wakeup('bark-walk-left');
}
step s2_check
{
	SELECT bark_index_check('bwl_idx', true);
	BEGIN;
	SET LOCAL enable_seqscan = on;
	SET LOCAL enable_indexscan = off;
	SET LOCAL enable_indexonlyscan = off;
	SET LOCAL enable_bitmapscan = off;
	SELECT array_agg(col ORDER BY col DESC) AS seqscan_desc FROM bwl
	 WHERE col % 1000 = 0;
	SELECT array_agg(col ORDER BY col DESC) AS seqscan_desc_14000 FROM bwl
	 WHERE col <= 14000 AND col % 1000 = 0;
	COMMIT;
}

# Leaf 6, the left sibling of leaf 7 that the scan has just read, splits
# once while the scan waits: one step right reaches the new page, whose
# right link is leaf 7.
permutation s1_explain s1_attach s1_scan s2_split_once s2_wakeup s1_detach
	s2_check

# Leaf 6 splits into many pages: four steps right do not reach the page
# whose right link is leaf 7, so the scan goes back to leaf 7, finds it live
# with a new left link, and starts again from that link.
permutation s1_attach s1_scan s2_split_many s2_wakeup s1_detach s2_check

# The scan has read leaf 5, every row of which (and of leaf 6) is dead to
# all, and waits to step left to leaf 4.  VACUUM deletes leaves 5 and 6.
# Leaf 4's right link now names leaf 7, the rightmost leaf, so walking right
# from it does not find leaf 5; leaf 5 is deleted, and so is the page its
# right link names, leaf 6.  The first live page to their right, leaf 7,
# took over their key space, and its left link leads back to leaf 4.
permutation s2_delete_mid s2_wait_prunable s1_attach s1_scan_14000 s2_vacuum
	s2_wakeup s1_detach s2_check

# The scan has read leaf 7 and waits to step left to leaf 6, every row of
# which is dead to all; VACUUM deletes leaf 6.  The deleted leaf keeps its
# right link to leaf 7, but it is not the page to return; its right link
# leads to leaf 7, the rightmost leaf, which ends the walk right.  Leaf 7 is
# live and its left link has moved to leaf 5, so the scan starts again from
# there.  A plain index scan this time, which holds no pin on leaf 7.
permutation s2_delete_6 s2_wait_prunable s1_attach s1_scan_plain s2_vacuum
	s2_wakeup s1_detach s2_check
