# Deleting a skinny subtree (bark_delete_subtree) while other backends
# read and write the pages it locks.
#
# The setup builds a two-level tree of 1 kB keys: the root (level 2) names
# level-1 pages of about seven leaves each.  C1, the root's second child,
# has children L .. T, and its right sibling C1R has R first.  Deleting
# the rows of every leaf of C1 lets VACUUM delete them all, the last one, T,
# together with C1 (one UNLINK_PAGE record with one internal level).  The
# rows of the other leaves of C1 are deleted in the setup and those leaves
# are deleted by a first VACUUM there, so that each permutation's VACUUM
# deletes T and C1 alone: the deletion of a leaf that is its parent's only
# child.  T holds the keys 730 .. 780.
#
# 1. A descent stops (bark-search-descend) after reading the root's
#    downlink to C1.  VACUUM deletes T and C1.  The descent then reads the
#    deleted C1, moves right to C1R, and from its first downlink, now minus
#    infinity, reaches R, which took T's keys over, and returns R's rows.
# 2. VACUUM stops holding every lock the deletion takes
#    (bark-delete-subtree-locked).  An insert into R waits for it, then
#    goes in once VACUUM is done: no deadlock, and the insert is found.
# 3. VACUUM finds the chain (bark-delete-subtree-found), which stops it
#    just before it locks T's left sibling, holding no lock.  An insert
#    into that leaf splits it and stops (bark-leave-leaf-split-incomplete)
#    holding it; the new right half is T's left sibling now.  The insert is
#    woken first: were VACUUM woken while the insert holds the leaf, it
#    would wait for a buffer lock, which the isolation tester cannot see,
#    and the tester would never wake the insert.  VACUUM then finds T's
#    left sibling moved, moves right to the new half and deletes T and C1.
# 4. VACUUM finds the chain, then an insert puts a row back on T: the
#    deletion declines under the locks, and T and C1 stay.
# 5. A backward scroll cursor positioned right of T steps back across the
#    deleted pages after VACUUM.
# 6. The level-1 entry cache across the recycling of C1's block.  s1 opens
#    a READ COMMITTED transaction and warms its level-1 table, which then
#    names C1 for keys from C1's low key on; between statements it holds no
#    snapshot, so nothing keeps C1's block from reuse.  It looks keys up
#    with a function whose plans it has cached and whose locks it holds, so
#    it accepts no invalidation messages: the relcache invalidation that
#    VACUUM's statistics update sends does not reach its level-1 table, as
#    it would not when the statistics do not change.  VACUUM deletes T and
#    C1, and a second VACUUM puts the deleted blocks in the free space map.  R was left mid-split before (its right half has no
#    downlink yet), so an insert into T's old range, which is now R's,
#    first finishes that split, which splits the full C1R: the new right
#    half of C1R takes the lowest free block, C1's, and is a level-1 page
#    again, for keys above C1's.  s1 then looks up the new key.  Its
#    table still names that block for the key; without the level-1 epoch,
#    the descent would start there, right of the key's page, and find
#    nothing.  The epoch moved, so the table is dropped, the descent starts
#    at the root, and the row is found.
# 7. An insert of a key in T's range, whose descent started at C1 from the
#    level-1 entry cache, stops after reading C1's downlink to T.  VACUUM
#    deletes T and C1; the insert moves right from T to R, which is full and
#    splits, and the search for R's parent starts at the deleted C1 and moves
#    right to C1R (bark_getstackbuf).
# 8. VACUUM finds the third leaf of Q, the root's fourth child, empty and
#    its downlink in the middle of Q; an insert then splits Q's fifth leaf,
#    and Q, full, splits in the middle, so that the leaf is the last child
#    of Q's left half.  VACUUM, under the locks, finds the downlink last and
#    declines; the leaf stays, empty, for a later VACUUM.

setup
{
	CREATE EXTENSION IF NOT EXISTS injection_points;
	CREATE EXTENSION IF NOT EXISTS amcheck;
	CREATE EXTENSION IF NOT EXISTS pageinspect;
	CREATE EXTENSION IF NOT EXISTS pg_freespacemap;
	CREATE FUNCTION sdk(int) RETURNS text LANGUAGE sql IMMUTABLE AS
		$$ SELECT lpad($1::text, 8, '0') || repeat('r', 1000) $$;
	CREATE TABLE sdi (k text COLLATE "C", n int) WITH (autovacuum_enabled = off);
	ALTER TABLE sdi ALTER COLUMN k SET STORAGE plain;
	INSERT INTO sdi SELECT sdk(g * 10), g * 10 FROM generate_series(1, 200) g;
	CREATE INDEX sdi_idx ON sdi USING bark (k);
	-- the j-th downlink of page p (from the right when j < 0)
	CREATE FUNCTION sdi_down(p int8, j int) RETURNS int8 LANGUAGE sql AS $$
		SELECT downlink FROM bark_page_items('sdi_idx', p) WHERE downlink IS NOT NULL
		 ORDER BY CASE WHEN j > 0 THEN itemoffset ELSE -itemoffset END
		 OFFSET abs(j) - 1 LIMIT 1 $$;
	CREATE FUNCTION sdi_c1() RETURNS int8 LANGUAGE sql AS $$
		SELECT sdi_down(root, 2) FROM bark_metap('sdi_idx') $$;
	-- the rows of every leaf of C1 but the last, T
	DELETE FROM sdi WHERE ctid IN
		(SELECT i.htid FROM generate_series(1, 7) j,
				LATERAL bark_page_items('sdi_idx', sdi_down(sdi_c1(), j)) i
		  WHERE sdi_down(sdi_c1(), j) <> sdi_down(sdi_c1(), -1));
	CREATE TABLE sdi_saved AS
		SELECT sdi_c1() AS c1, sdi_down(sdi_c1(), -1) AS t,
			   (SELECT min(n) FROM sdi s JOIN bark_page_items('sdi_idx', sdi_down(sdi_c1(), -1)) i
				  ON s.ctid = i.htid) AS tlo,
			   (SELECT max(n) FROM sdi s JOIN bark_page_items('sdi_idx', sdi_down(sdi_c1(), -1)) i
				  ON s.ctid = i.htid) AS thi,
			   -- the last row of C1R's second child
			   (SELECT max(n) FROM sdi s, bark_metap('sdi_idx') m,
					   LATERAL bark_page_items('sdi_idx', sdi_down(sdi_down(m.root, 3), 2)) i
				 WHERE s.ctid = i.htid) AS r2hi,
			   -- Q, the root's fourth child (six leaves, full): its third leaf,
			   -- and the last row of its fifth
			   (SELECT sdi_down(sdi_down(m.root, 4), 3) FROM bark_metap('sdi_idx') m) AS q3,
			   (SELECT max(n) FROM sdi s, bark_metap('sdi_idx') m,
					   LATERAL bark_page_items('sdi_idx', sdi_down(sdi_down(m.root, 4), 5)) i
				 WHERE s.ctid = i.htid) AS q5hi;
	CREATE FUNCTION sdi_tlo() RETURNS int LANGUAGE sql STABLE AS $$ SELECT tlo FROM sdi_saved $$;
	CREATE FUNCTION sdi_thi() RETURNS int LANGUAGE sql STABLE AS $$ SELECT thi FROM sdi_saved $$;
	CREATE FUNCTION sdi_r2hi() RETURNS int LANGUAGE sql STABLE AS $$ SELECT r2hi FROM sdi_saved $$;
	-- How many rows of key T's low key || suffix the index finds.  Its plans
	-- are cached once it has run in a transaction, and with its locks held
	-- it then accepts no invalidation messages (see permutation 6).
	CREATE FUNCTION sdi_probe(suffix text) RETURNS bigint LANGUAGE plpgsql AS $$
	DECLARE t int; c bigint;
	BEGIN
		SELECT tlo INTO t FROM sdi_saved;
		SELECT count(*) INTO c FROM sdi WHERE k = sdk(t) || suffix;
		RETURN c;
	END $$;
	-- point lookups of every key, each a descent of its own
	CREATE FUNCTION sdi_warm() RETURNS bigint LANGUAGE plpgsql AS $$
	DECLARE c bigint := 0;
	BEGIN
		PERFORM set_config('enable_seqscan', 'off', true);
		PERFORM set_config('enable_bitmapscan', 'off', true);
		FOR g IN 1..200 LOOP
			c := c + (SELECT count(*) FROM sdi WHERE k = sdk(g * 10));
		END LOOP;
		RETURN c;
	END $$;
	CREATE FUNCTION sdi_type(blk int8) RETURNS text LANGUAGE sql AS $$
		SELECT type FROM bark_page_stats('sdi_idx', blk) $$;
	-- What happened to T and C1, and is the index sound.
	CREATE FUNCTION sdi_state() RETURNS text LANGUAGE sql AS $$
		SELECT format('T %s, C1 %s', sdi_type(t), sdi_type(c1))
		  FROM sdi_saved, (SELECT bark_index_parent_check('sdi_idx', true)) x $$;
	-- The rows the index returns for keys >= k (index scans only) against
	-- a sequential scan.
	CREATE FUNCTION sdi_same() RETURNS bool LANGUAGE plpgsql AS $$
	DECLARE a text; b text;
	BEGIN
		PERFORM set_config('enable_seqscan', 'off', true);
		PERFORM set_config('enable_bitmapscan', 'off', true);
		SELECT string_agg(n::text, ',') INTO a FROM (SELECT n FROM sdi WHERE k > '' ORDER BY k) s;
		PERFORM set_config('enable_seqscan', 'on', true);
		PERFORM set_config('enable_indexscan', 'off', true);
		PERFORM set_config('enable_indexonlyscan', 'off', true);
		SELECT string_agg(n::text, ',') INTO b FROM (SELECT n FROM sdi ORDER BY k) s;
		RETURN a = b;
	END $$;
}
setup
{
	VACUUM sdi;
}
# Recycle the leaves the first VACUUM deleted and use them up (with keys
# right of every other), so that the free space map is empty when a
# permutation begins.
setup
{
	SELECT pg_current_xact_id() IS NOT NULL;
}
setup
{
	VACUUM sdi;
}
setup
{
	INSERT INTO sdi SELECT sdk(5000 + g), 5000 + g FROM generate_series(1, 60) g;
}

teardown
{
	DROP TABLE sdi, sdi_saved;
	DROP TABLE IF EXISTS sdi_prev;
	DROP FUNCTION sdk(int), sdi_down(int8, int), sdi_c1(), sdi_tlo(), sdi_thi(),
		sdi_r2hi(), sdi_probe(text), sdi_warm(), sdi_type(int8), sdi_state(),
		sdi_same();
}

session s1
setup
{
	SELECT injection_points_set_local();
	SET enable_seqscan = off;
	SET enable_bitmapscan = off;
}
step s1_attach_descend	{ SELECT injection_points_attach('bark-search-descend', 'wait'); }
# pg_backend_pid() is parallel restricted, so the scan runs in this backend,
# where the local injection point is attached.  From T's old range to R's
# first rows.
step s1_range
{
	SELECT n FROM sdi WHERE k >= sdk(sdi_tlo()) AND k < sdk(sdi_thi() + 30)
	   AND pg_backend_pid() <> 0 ORDER BY k;
}
# A lookup in C1's range, from the root, makes the next descent there start
# at C1 (the level-1 entry cache); the insert then stops after reading C1's
# downlink to T, with C1 as the first frame of its path.
step s1_warm_c1	{ SELECT count(*) FROM sdi WHERE k = sdk(sdi_tlo()); }
step s1_attach_both
{
	SELECT injection_points_attach('bark-l1-start', 'notice');
	SELECT injection_points_attach('bark-search-descend', 'wait');
}
step s1_insert_w	{ INSERT INTO sdi VALUES (sdk(sdi_tlo()) || 'w', 0); }
step s1_state	{ SELECT sdi_state(), sdi_same(); }
step s1_cursor
{
	BEGIN;
	DECLARE c SCROLL CURSOR FOR SELECT n FROM sdi ORDER BY k;
	FETCH ABSOLUTE 90 FROM c;
}
step s1_back
{
	FETCH BACKWARD 3 FROM c;
	FETCH RELATIVE -70 FROM c;
	COMMIT;
}
step s1_begin	{ BEGIN ISOLATION LEVEL READ COMMITTED; }
# Every lookup below C1's high key and from C1's low key on starts at C1,
# once the root's descent has recorded it.
step s1_warm	{ SELECT sdi_warm(); }
step s1_attach_l1	{ SELECT injection_points_attach('bark-l1-start', 'notice'); }
step s1_detach_l1	{ SELECT injection_points_detach('bark-l1-start'); }
step s1_find_new	{ SELECT sdi_probe('y') AS found; }
step s1_commit	{ COMMIT; }

session s2
setup
{
	SELECT injection_points_set_local();
}
step s2_attach_found	{ SELECT injection_points_attach('bark-delete-subtree-found', 'wait'); }
step s2_attach_locked	{ SELECT injection_points_attach('bark-delete-subtree-locked', 'wait'); }
step s2_delete_t	{ DELETE FROM sdi USING sdi_saved WHERE n BETWEEN tlo AND thi; }
step s2_delete_q3
{
	DELETE FROM sdi WHERE ctid IN
		(SELECT i.htid FROM sdi_saved, bark_page_items('sdi_idx', q3) i WHERE i.htid IS NOT NULL);
}
step s2_q3	{ SELECT sdi_type(q3) AS q3 FROM sdi_saved; }
step s2_vacuum		{ VACUUM sdi; }
step s2_xid			{ SELECT pg_current_xact_id() IS NOT NULL AS xid_assigned; }
step s2_state		{ SELECT sdi_state(), sdi_same(); }
step s2_free
{
	SELECT count(*) AS free_pages FROM pg_freespace('sdi_idx') WHERE avail > 0;
}
# A key of T's old range, which is R's now.  Its descent finishes R's split,
# whose downlink fits in C1R (whose first downlink the deletion shortened).
# Then a key of R2's: finishing R2's split adds a downlink to C1R, which is
# full now and splits, and its new right half takes the lowest free block,
# C1's.
step s2_refill
{
	INSERT INTO sdi VALUES (sdk(sdi_tlo()) || 'y', 0);
	INSERT INTO sdi VALUES (sdk(sdi_r2hi()) || 'b', 0);
	SELECT sdi_type(c1) AS c1_now, (SELECT bark_level FROM bark_page_stats('sdi_idx', c1)) AS c1_level
	  FROM sdi_saved;
}
step s2_prev
{
	CREATE TABLE sdi_prev AS SELECT bark_prev AS p FROM sdi_saved, bark_page_stats('sdi_idx', t);
}
step s2_moved
{
	SELECT bark_prev <> p AS left_sibling_moved
	  FROM sdi_saved, sdi_prev, bark_page_stats('sdi_idx', t);
}

session s3
setup
{
	SELECT injection_points_set_local();
}
step s3_insert_r	{ INSERT INTO sdi VALUES (sdk(sdi_thi() + 10) || 'x', 0); }
step s3_insert_t	{ INSERT INTO sdi VALUES (sdk(sdi_tlo()) || 'z', 0); }
# R and R2, C1R's first two children, are full: one more key splits each,
# and the split stops with an error after the right half is linked, before
# its downlink is in C1R.
step s3_split_r
{
	SELECT injection_points_attach('bark-leave-leaf-split-incomplete', 'error');
	INSERT INTO sdi VALUES (sdk(sdi_thi() + 10) || 'a', 0);
}
step s3_split_r2	{ INSERT INTO sdi VALUES (sdk(sdi_r2hi()) || 'a', 0); }
step s3_state	{ SELECT sdi_state(), sdi_same(); }
# One key after the last of Q's fifth leaf, which is full: the leaf splits,
# and its new downlink splits Q, also full, in the middle, after its third.
step s3_split_q	{ INSERT INTO sdi SELECT sdk(q5hi) || 'a', 0 FROM sdi_saved; }
step s3_detach_split	{ SELECT injection_points_detach('bark-leave-leaf-split-incomplete'); }
step s3_attach_split	{ SELECT injection_points_attach('bark-leave-leaf-split-incomplete', 'wait'); }
# Keys of T's left neighbour L (just above its last row's, below its high
# key): enough to split it, inserted in one statement, which stops in the
# split.
step s3_split_left
{
	INSERT INTO sdi SELECT sdk((SELECT max(n) FROM sdi WHERE n < sdi_tlo())) || g, 0
	  FROM generate_series(1, 6) g;
}

session s4
step s4_wake_descend
{
	SELECT injection_points_detach('bark-search-descend');
	SELECT injection_points_wakeup('bark-search-descend');
}
step s4_wake_found
{
	SELECT injection_points_detach('bark-delete-subtree-found');
	SELECT injection_points_wakeup('bark-delete-subtree-found');
}
step s4_wake_locked
{
	SELECT injection_points_detach('bark-delete-subtree-locked');
	SELECT injection_points_wakeup('bark-delete-subtree-locked');
}
step s4_wake_split
{
	SELECT injection_points_detach('bark-leave-leaf-split-incomplete');
	SELECT injection_points_wakeup('bark-leave-leaf-split-incomplete');
}

# 1: a descent holding a downlink to the deleted C1
permutation s2_delete_t s1_attach_descend s1_range s2_vacuum s4_wake_descend
	s1_state

# 2: an insert into R waits for the deletion's locks
permutation s2_attach_locked s2_delete_t s2_vacuum s3_insert_r(*, s2_vacuum)
	s4_wake_locked s3_state

# 3: the deletion waits for a split of T's left neighbour, and moves right
permutation s2_prev s2_attach_found s2_delete_t s2_vacuum s3_attach_split
	s3_split_left(s2_vacuum) s4_wake_split s4_wake_found s2_moved s3_state

# 4: T is refilled before the deletion locks it
permutation s2_attach_found s2_delete_t s2_vacuum s3_insert_t s4_wake_found
	s2_state

# 5: a backward cursor across the deleted pages
permutation s2_delete_t s1_cursor s2_vacuum s1_back s2_state

# 6: the level-1 entry cache across the reuse of C1's block
permutation s2_free s1_begin s1_warm s1_attach_l1 s1_find_new s1_detach_l1
	s3_split_r s3_split_r2 s3_detach_split s2_delete_t s2_vacuum s2_xid
	s2_vacuum s2_free s2_refill s1_find_new s1_commit s2_state

# 7: an insert whose path names C1, deleted before the insert splits R:
# the split's parent search moves right from C1 to C1R
permutation s2_delete_t s1_warm_c1 s1_attach_both s1_insert_w s2_vacuum
	s4_wake_descend s1_state

# 8: the subtree parent splits between the search and the locks, and
# Q's third leaf, empty, is then its page's last child: declined
permutation s2_attach_found s2_delete_q3 s2_vacuum s3_split_q s4_wake_found
	s2_q3 s2_state
