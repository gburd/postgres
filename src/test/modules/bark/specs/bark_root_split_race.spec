# A split whose parent level gained a root while it was in progress.
#
# A split inserts its downlink into the parent page its descent recorded.
# When that parent was the root and is full, it splits in turn with no stack
# above it (bark_insert_parent with stack NULL).  If the root is still the
# page being split, a new root is made; if another backend has meanwhile
# split the root, the parent's parent is found from the leftmost page of
# that level (bark_get_leftmost_at_level), and bark_getstackbuf moves right
# from there to the downlink, as nbtree's _bt_insert_parent does.
#
# Keys are 968-byte strings, so a page holds about eight entries or pivots
# and the tree is a few pages wide.  The setup builds a two-level index; the
# leftmost leaf X is full.  s1 inserts below every key, splits X and stops
# with the split's downlink not yet in the root
# (bark-leave-leaf-split-incomplete), holding X.  s2 inserts descending keys
# into the second leaf, so each of its leaf splits adds a downlink just after
# X's, until the root has split once (first permutation) or twice (second),
# and then until the page holding X's downlink has no room for one more.
# Woken, s1 splits that page with no stack above it while the root is
# another page: in the first permutation the root is that page's parent, in
# the second the parent is found one level below the root, and in the third
# two levels below, through the leftmost page of a level with several pages.
#
# The last permutation splits the rightmost leaf instead.  Its downlink is
# the root's last, and moves to the right half when s2 splits the root
# (which, being rightmost, keeps 70% on the left), so bark_getstackbuf finds
# the offset its descent recorded past the end of that page, searches it,
# and moves right to the downlink.
#
# The index must pass bark_index_check with heapallindexed, every row must
# come back from an index scan and a sequential scan, and no page may still
# carry BARK_INCOMPLETE_SPLIT.

setup
{
	CREATE EXTENSION injection_points;
	CREATE EXTENSION amcheck;
	CREATE EXTENSION pageinspect;
	CREATE FUNCTION rs_key(int) RETURNS text LANGUAGE sql IMMUTABLE AS
		$$ SELECT lpad($1::text, 8, '0') || string_agg(md5($1::text || g::text), '')
			 FROM generate_series(1, 30) g $$;
	CREATE TABLE rs (k text COLLATE "C") WITH (autovacuum_enabled = off);
	INSERT INTO rs SELECT rs_key(g * 100000) FROM generate_series(1, 30) g;
	CREATE INDEX rs_idx ON rs USING bark (k);

	-- The leftmost page at level 1, from the root down, reading only
	-- internal pages (s1 holds the leftmost leaf locked).
	CREATE FUNCTION rs_level1() RETURNS int8 LANGUAGE plpgsql AS $$
	DECLARE
		b int8 := (SELECT root FROM bark_metap('rs_idx'));
	BEGIN
		WHILE (SELECT bark_level FROM bark_page_stats('rs_idx', b)) > 1 LOOP
			b := (SELECT downlink FROM bark_page_items('rs_idx', b)
				   WHERE downlink IS NOT NULL ORDER BY itemoffset LIMIT 1);
		END LOOP;
		RETURN b;
	END $$;

	-- Insert descending keys just above the second leaf's low bound until
	-- the root is at `level`, then until the leftmost level-1 page cannot
	-- take one more pivot.
	CREATE FUNCTION rs_grow(target int) RETURNS text LANGUAGE plpgsql AS $$
	DECLARE
		n int := 899999;
		p int8;
	BEGIN
		WHILE (SELECT m.level FROM bark_metap('rs_idx') m) < target LOOP
			IF n <= 800000 THEN
				RAISE EXCEPTION 'ran out of keys';
			END IF;
			INSERT INTO rs VALUES (rs_key(n));
			n := n - 1;
		END LOOP;
		LOOP
			p := rs_level1();
			EXIT WHEN (SELECT free_size FROM bark_page_stats('rs_idx', p)) <
				(SELECT (max(itemlen) + 7) / 8 * 8
				   FROM bark_page_items('rs_idx', p));
			IF n <= 800000 THEN
				RAISE EXCEPTION 'ran out of keys';
			END IF;
			INSERT INTO rs VALUES (rs_key(n));
			n := n - 1;
		END LOOP;
		RETURN format('root at level %s', target);
	END $$;
}

teardown
{
	DROP TABLE rs;
	DROP FUNCTION rs_key(int), rs_level1(), rs_grow(int);
	DROP EXTENSION pageinspect;
	DROP EXTENSION amcheck;
	DROP EXTENSION injection_points;
}

session s1
setup
{
	SELECT injection_points_set_local();
	SELECT injection_points_attach('bark-leave-leaf-split-incomplete', 'wait');
}
step s1_split	{ INSERT INTO rs SELECT rs_key(g) FROM generate_series(1, 20) g; }
step s1_split_right
{
	INSERT INTO rs SELECT rs_key(3000000 + g) FROM generate_series(1, 20) g;
}
step s1_check
{
	SELECT bark_index_check('rs_idx', true);
	SELECT count(*) AS incomplete_pages
	  FROM bark_multi_page_stats('rs_idx', 1, -1)
	 WHERE 'incomplete_split' = ANY (flags);
	SET enable_seqscan = off;
	SET enable_bitmapscan = off;
	SELECT count(*) AS index_count, sum(hashtext(k)) AS index_hash
	  FROM rs WHERE k >= '';
	RESET enable_seqscan;
	RESET enable_bitmapscan;
	SET enable_indexscan = off;
	SET enable_indexonlyscan = off;
	SET enable_bitmapscan = off;
	SELECT count(*) AS heap_count, sum(hashtext(k)) AS heap_hash FROM rs;
	RESET enable_indexscan;
	RESET enable_indexonlyscan;
	RESET enable_bitmapscan;
}

session s2
step s2_grow1	{ SELECT rs_grow(2); }
step s2_grow2	{ SELECT rs_grow(3); }
step s2_grow3	{ SELECT rs_grow(4); }

session s3
step s3_release
{
	SELECT injection_points_detach('bark-leave-leaf-split-incomplete');
	SELECT injection_points_wakeup('bark-leave-leaf-split-incomplete');
}

permutation s1_split s2_grow1 s3_release s1_check
permutation s1_split s2_grow2 s3_release s1_check
permutation s1_split s2_grow3 s3_release s1_check
permutation s1_split_right s2_grow1 s3_release s1_check
