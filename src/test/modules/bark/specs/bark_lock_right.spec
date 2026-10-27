# A row's entries meet a split of the next leaf (bark_lock_right).
#
# An insert of a row with an extracted column places the row's entries in key
# order in one pass: once an entry is on a leaf, the next entry, if it sorts
# past that leaf's high key, is taken to the right sibling with the leaf
# still locked (bark_lock_right), left to right as every writer locks.
#
# The setup builds a two-level index of single-element arrays and picks two
# keys: k1, the last key on the leaf L just left of the rightmost leaf R, and
# k2, a new key just above R's first key.  The row {k1, k2} puts its first
# entry on L and its second on R, so it locks R from L.  s1 splits R by
# inserting above every key.
#
# The first permutation stops s1 in the middle of that split, holding R
# locked with BARK_INCOMPLETE_SPLIT set (bark-leave-leaf-split-incomplete).
# s2 must wait for R's lock instead of finishing s1's split: s3 checks that
# it is waiting on a buffer lock before waking s1, and
# bark-finish-incomplete-split is attached in notice mode in s2, so a NOTICE
# in that permutation would mean s2 inserted a downlink for s1's split.
#
# The second permutation makes s1's split fail at the same point, which
# leaves R flagged with no backend completing it.  s2 then finds the flag
# under R's lock and finishes the split from L's descent path before placing
# its second entry (one NOTICE).
#
# bark_index_check cannot check heapallindexed for an index with an extracted
# column, and a scan would hide repeated entries by returning each row once;
# so the (key, heap TID) members the index holds for live rows are compared
# both ways with those the heap's rows give, the index is checked, and no leaf may
# still carry the flag.  The insert whose split failed in the second
# permutation leaves one entry for its aborted row (the split, entry
# included, was already written), as a btree would until VACUUM removes it:
# dead_entries counts it.  The branches of bark_lock_right that step over a deleted
# or half-dead right sibling are not reached: VACUUM rewrites a leaf's right
# link under that leaf's lock when it deletes the leaf's right sibling, so a
# right link read under the lock never names a deleted page, and no code sets
# BARK_HALF_DEAD.

setup
{
	CREATE EXTENSION injection_points;
	CREATE EXTENSION amcheck;
	CREATE EXTENSION pageinspect;
	CREATE EXTENSION bark_multikey;
	CREATE TABLE lr (a int4[]) WITH (autovacuum_enabled = off);
	INSERT INTO lr SELECT ARRAY[g * 10] FROM generate_series(1, 2000) g;
	CREATE INDEX lr_a ON lr USING bark (a bark_int4_array_ops);

	-- k1 and k2, from the heap rows of the leaf entries' TIDs.  Item 1 of
	-- L is its high key; R, the rightmost leaf, has none.
	CREATE TABLE lr_keys AS
	WITH r AS (SELECT blkno, bark_prev
				 FROM bark_multi_page_stats('lr_a', 1, -1)
				WHERE type = 'leaf' AND bark_next = 0)
	SELECT (SELECT max(lr.a[1])
			  FROM bark_page_items('lr_a', (SELECT bark_prev FROM r)) i
			  JOIN lr ON lr.ctid = i.htid
			 WHERE i.itemoffset > 1) AS k1,
		   (SELECT min(lr.a[1]) + 1
			  FROM bark_page_items('lr_a', (SELECT blkno FROM r)) i
			  JOIN lr ON lr.ctid = i.htid) AS k2;

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
	DROP TABLE lr, lr_keys;
	DROP FUNCTION bark_wait_for_buffer_lock(text);
	DROP EXTENSION bark_multikey;
	DROP EXTENSION pageinspect;
	DROP EXTENSION amcheck;
	DROP EXTENSION injection_points;
}

session s1
setup
{
	SELECT injection_points_set_local();
}
step s1_attach_wait
{
	SELECT injection_points_attach('bark-leave-leaf-split-incomplete', 'wait');
}
step s1_split	{ INSERT INTO lr SELECT ARRAY[20000 + g] FROM generate_series(1, 1000) g; }
step s1_abandon
{
	SELECT injection_points_attach('bark-leave-leaf-split-incomplete', 'error');
	DO $$
	BEGIN
		FOR n IN 1..1000 LOOP
			BEGIN
				INSERT INTO lr VALUES (ARRAY[20000 + n]);
			EXCEPTION WHEN OTHERS THEN
				RETURN;
			END;
		END LOOP;
		RAISE EXCEPTION 'no split was interrupted';
	END $$;
	SELECT injection_points_detach('bark-leave-leaf-split-incomplete');
	SELECT count(*) AS incomplete_leaves
	  FROM bark_multi_page_stats('lr_a', 1, -1)
	 WHERE 'incomplete_split' = ANY (flags);
}

session s2
setup
{
	SELECT injection_points_set_local();
	SELECT injection_points_attach('bark-finish-incomplete-split', 'notice');
}
step s2_insert	{ INSERT INTO lr SELECT ARRAY[k1, k2] FROM lr_keys; }
step s2_check
{
	SELECT bark_index_check('lr_a');
	SELECT count(*) AS incomplete_leaves
	  FROM bark_multi_page_stats('lr_a', 1, -1)
	 WHERE 'incomplete_split' = ANY (flags);
	WITH heap AS (SELECT DISTINCT e AS key, ctid AS tid FROM lr, unnest(a) e),
		 idx AS (SELECT key, tid FROM bark_multikey_entries('lr_a')
				  WHERE NOT marker AND tid IN (SELECT ctid FROM lr))
	SELECT (SELECT count(*) FROM (TABLE heap EXCEPT TABLE idx) x) AS missing,
		   (SELECT count(*) FROM (TABLE idx EXCEPT TABLE heap) x) AS extra,
		   (SELECT count(*) FROM idx) = (SELECT count(*) FROM heap) AS no_repeats,
		   (SELECT count(*) FROM bark_multikey_entries('lr_a')
			 WHERE tid NOT IN (SELECT ctid FROM lr)) AS dead_entries;
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
permutation s1_attach_wait s1_split s2_insert(*, s1_split) s3_release s2_check
permutation s1_abandon s2_insert s2_check
