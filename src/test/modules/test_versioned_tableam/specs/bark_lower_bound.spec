# Scans and inserts across the lowering of a BARK leaf's tagged lower bound
# (bark_lower_bound).
#
# Row 1 (k = 5) has a run of 600 entries, tagged 2, 4, .., 1200, which fills
# whole leaves; the split inside the run leaves the leftmost leaf P1 a
# tagged high key (5, L, t0), and its right sibling P2 BARK_LOWBOUND_TAGGED.
# VACUUM is told P1's entries are dead and P2's first entry may be untagged,
# so after its scan it lowers P1's high key and P2's downlink to (5, L) and
# untags that entry.  It stops at bark-lower-bound-locked, holding the parent
# and both leaves, just before the change.
#
# 1. A forward cursor that read P1 before VACUUM carries on across the
#    lowering, blocking on P2's lock, and returns every entry once.
# 2. A backward cursor that read P2 before VACUUM carries on, blocking on
#    P1's lock, and returns every entry it still reaches (P1's are dead by
#    then) once.
# 3. An insert of (5, L, t0 - 1), which belongs right of the new bound,
#    reads the old downlink to P1 and stops (bark-search-descend); VACUUM
#    lowers the bound; the insert finds P1's new high key and moves right
#    to P2 (Lehman and Yao).
# 4. The same insert, started while VACUUM holds the locks, waits on the
#    parent, then follows the new downlink to P2.
# Each ends with bark_index_parent_check.  The cursors' qual is k <= 5, not
# k = 5, under which ORDER BY k DESC would be redundant and the scan
# forward.  A buffer-lock wait is invisible
# to isolationtester, hence (*), and the step after it waits for it
# (wait_for_buffer_lock) before waking VACUUM.

setup
{
	CREATE EXTENSION test_versioned_tableam;
	CREATE EXTENSION injection_points;
	CREATE EXTENSION amcheck;
	CREATE EXTENSION pageinspect;
}

# xmins above the run's tags; one statement, so the loop may commit
setup
{
	DO $$ BEGIN FOR i IN 1..1300 LOOP PERFORM pg_current_xact_id(); COMMIT; END LOOP; END $$;
}

setup
{
	CREATE FUNCTION leaves(idx regclass)
	  RETURNS TABLE (blk int, prev int8, flags text[], hitag int8, firsttag int8,
	                 mintag int8, maxtag int8, n int8)
	  LANGUAGE sql AS $$
	  SELECT b::int, s.bark_prev, s.flags,
	         (SELECT i.tag FROM bark_page_items(idx::text, b) i
	           WHERE s.bark_next <> 0 AND i.itemoffset = 1),
	         (SELECT i.tag FROM bark_page_items(idx::text, b) i
	           WHERE i.shape = 'SINGLE' ORDER BY i.itemoffset LIMIT 1),
	         (SELECT min(i.tag) FROM bark_page_items(idx::text, b) i WHERE i.shape = 'SINGLE'),
	         (SELECT max(i.tag) FROM bark_page_items(idx::text, b) i WHERE i.shape = 'SINGLE'),
	         (SELECT count(*) FROM bark_page_items(idx::text, b) i WHERE i.shape = 'SINGLE')
	    FROM generate_series(1, pg_relation_size(idx) / 8192 - 1) b,
	         LATERAL bark_page_stats(idx::text, b) s
	   WHERE s.type = 'leaf' $$;
	-- the rows a cursor has left, and a cursor moved n rows on
	CREATE FUNCTION drain(c refcursor) RETURNS int8 LANGUAGE plpgsql AS $$
	DECLARE n int8 := 0; x int;
	BEGIN
		LOOP FETCH c INTO x; EXIT WHEN NOT FOUND; n := n + 1; END LOOP;
		RETURN n;
	END $$;
	CREATE FUNCTION skip(c refcursor, n int) RETURNS bool LANGUAGE plpgsql AS $$
	BEGIN MOVE FORWARD n FROM c; RETURN FOUND; END $$;
	-- wait until session `sess` is blocked on a buffer lock, or idle
	CREATE FUNCTION wait_for_buffer_lock(sess text) RETURNS text
	LANGUAGE plpgsql AS $$
	DECLARE r text;
	BEGIN
		LOOP
			PERFORM pg_stat_clear_snapshot();
			SELECT CASE WHEN wait_event_type = 'Buffer' THEN 'blocked on buffer lock'
			            WHEN state = 'idle' THEN 'not blocked' END
			  INTO r FROM pg_stat_activity WHERE application_name LIKE '%/' || sess;
			IF r IS NOT NULL THEN RETURN r; END IF;
			PERFORM pg_sleep(0.01);
		END LOOP;
	END $$;

	CREATE TABLE tl (id int, k int) USING heap_versioned;
	INSERT INTO tl SELECT g, CASE WHEN g = 1 THEN 5 ELSE 10 * g END
	  FROM generate_series(1, 300) g;
	SET test_versioned_tableam.build_tag_percent = 100;
	CREATE INDEX tl_k ON tl USING bark (k);
	SET test_versioned_tableam.stale_tags = live;
	SELECT count(test_versioned_insert_entry('tl_k', ctid, 2 * g))
	  FROM tl, generate_series(1, 600) g WHERE id = 1;
	RESET test_versioned_tableam.stale_tags;

	-- P1, P2, P1's tags, P2's first tag, the entries of row 1, those on P1,
	-- and those right of P2, plus one
	CREATE TABLE exp AS
	  SELECT p1.blk AS p1, p2.blk AS p2, p1.mintag AS lo, p1.maxtag AS hi,
	         p2.firsttag AS first, p1.hitag AS t0,
	         (SELECT sum(n) FROM leaves('tl_k')) - 299 AS n0, p1.n AS n1,
	         (SELECT sum(n) FROM leaves('tl_k') l WHERE l.mintag > p2.maxtag) - 299 + 1 AS nskip
	    FROM leaves('tl_k') p2 JOIN leaves('tl_k') p1 ON p1.blk = p2.prev
	   WHERE 'lowbound_tagged' = ANY (p2.flags) AND p1.prev = 0;
}

teardown
{
	DROP TABLE tl, exp;
	DROP FUNCTION leaves(regclass), drain(refcursor), skip(refcursor, int),
	  wait_for_buffer_lock(text);
	DROP EXTENSION pageinspect;
	DROP EXTENSION amcheck;
	DROP EXTENSION injection_points;
	DROP EXTENSION test_versioned_tableam;
}

session s1
setup
{
	SET test_versioned_tableam.stale_tags = live;
	SELECT set_config('test_versioned_tableam.stale_dead_min', lo::text, false),
	       set_config('test_versioned_tableam.stale_dead_max', hi::text, false),
	       set_config('test_versioned_tableam.untag_tag', first::text, false) IS NOT NULL AS set
	  FROM exp;
	SELECT injection_points_set_local();
}
step s1_attach	{ SELECT injection_points_attach('bark-lower-bound-locked', 'wait'); }
step s1_vacuum	{ VACUUM tl; }
step s1_check
{
	SELECT l1.hitag IS NULL AS p1_hikey_untagged,
	       'lowbound_tagged' <> ALL (l2.flags) AS p2_flag_clear,
	       l2.firsttag IS NULL AS p2_first_untagged,
	       l1.n AS p1_entries
	  FROM exp, leaves('tl_k') l1, leaves('tl_k') l2 WHERE l1.blk = p1 AND l2.blk = p2;
	SELECT count(*) AS lowered_downlinks
	  FROM exp, generate_series(1, pg_relation_size('tl_k') / 8192 - 1) b,
	       LATERAL bark_page_stats('tl_k', b) s, LATERAL bark_page_items('tl_k', b) i
	 WHERE s.bark_level = 1 AND i.downlink = p2 AND i.tag IS NULL AND i.htid IS NOT NULL;
	SELECT bark_index_parent_check('tl_k', true);
}
step s1_inserted
{
	SELECT l.blk = p2 AS on_p2
	  FROM exp, leaves('tl_k') l, bark_page_items('tl_k', l.blk) i
	 WHERE i.tag = t0 - 1;
}

session s2
setup
{
	SET test_versioned_tableam.stale_tags = live;
	SET enable_seqscan = off;
	SET enable_bitmapscan = off;
	SET enable_sort = off;
}
step s2_open
{
	BEGIN;
	DECLARE f CURSOR FOR SELECT id FROM tl WHERE k <= 5 ORDER BY k;
	FETCH 1 FROM f;
}
step s2_drain	{ SELECT drain('f') = (SELECT n0 - 1 FROM exp) AS forward_each_once; COMMIT; }
step s2_descend_attach
{
	SELECT injection_points_set_local();
	SELECT injection_points_attach('bark-search-descend', 'wait');
}
# row 1 is at (0,1); by TID, since a scan for it could descend tl_k
step s2_insert
{
	SELECT test_versioned_insert_entry('tl_k', '(0,1)', t0 - 1) IS NOT NULL AS inserted FROM exp;
}

session s3
setup
{
	SET test_versioned_tableam.stale_tags = live;
	SET enable_seqscan = off;
	SET enable_bitmapscan = off;
	SET enable_sort = off;
}
step s3_open
{
	BEGIN;
	DECLARE b CURSOR FOR SELECT id FROM tl WHERE k <= 5 ORDER BY k DESC;
	SELECT skip('b', (SELECT nskip::int FROM exp));
}
step s3_drain
{
	SELECT drain('b') = (SELECT n0 - n1 - nskip FROM exp) AS backward_each_once;
	COMMIT;
}

session s4
step s4_wakeup_s2
{
	SELECT wait_for_buffer_lock('s2');
	SELECT injection_points_detach('bark-lower-bound-locked');
	SELECT injection_points_wakeup('bark-lower-bound-locked');
}
step s4_wakeup_s3
{
	SELECT wait_for_buffer_lock('s3');
	SELECT injection_points_detach('bark-lower-bound-locked');
	SELECT injection_points_wakeup('bark-lower-bound-locked');
}
step s4_wakeup_descend
{
	SELECT injection_points_detach('bark-search-descend');
	SELECT injection_points_wakeup('bark-search-descend');
}

# 1, 2. scans across the lowering
permutation s2_open s1_attach s1_vacuum s2_drain(*, s1_vacuum) s4_wakeup_s2 s1_check
permutation s3_open s1_attach s1_vacuum s3_drain(*, s1_vacuum) s4_wakeup_s3 s1_check
# 3. an insert that read the old downlink
permutation s2_descend_attach s2_insert s1_vacuum s4_wakeup_descend s1_check s1_inserted
# 4. an insert that waits for the lowering's locks
permutation s1_attach s1_vacuum s2_insert(*, s1_vacuum) s4_wakeup_s2 s1_check s1_inserted
