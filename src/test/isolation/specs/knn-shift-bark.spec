# A BARK KNN scan (ORDER BY a <~> const) merges two cursors, one walking the
# leaf chain toward higher keys and one toward lower keys.  Each cursor reads
# a leaf a page at a time: it copies the page's matching entries when it first
# reads the page, then returns them from that copy, and steps to the next page
# by the sibling link it saved.  Concurrent inserts and splits on either
# cursor's leaves, and VACUUM deleting the leaves next to them, cannot make the
# scan repeat or skip rows or return them out of distance order.
#
# The base code resumed each cursor by page offset with the lock released
# between rows, so inserts that shifted a leaf's items made the scan return
# some rows twice and skip others.
#
# Every key in the tables is a multiple of 10 and every key s2 inserts is not,
# so "fetched = distinct_rows = the table's size, foreign = 0" means the
# cursor returned exactly the rows its snapshot sees.

setup
{
	CREATE TABLE knn_shift (a int) WITH (autovacuum_enabled = off);
	INSERT INTO knn_shift SELECT g * 10 FROM generate_series(1, 100) g;
	CREATE INDEX knn_shift_idx ON knn_shift USING bark (a);
	CREATE TABLE knn_shift_wide (a int) WITH (autovacuum_enabled = off);
	INSERT INTO knn_shift_wide SELECT g * 10 FROM generate_series(1, 3000) g;
	CREATE INDEX knn_shift_wide_idx ON knn_shift_wide USING bark (a);
	CREATE TABLE knn_shift_vac (a int) WITH (autovacuum_enabled = off);
	INSERT INTO knn_shift_vac SELECT g * 10 FROM generate_series(1, 3000) g;
	CREATE INDEX knn_shift_vac_idx ON knn_shift_vac USING bark (a);
	ANALYZE knn_shift, knn_shift_wide, knn_shift_vac;
	CREATE TABLE knn_shift_got (n serial, a int, d float8);
	CREATE FUNCTION knn_shift_fetch(cur refcursor, maxrows int) RETURNS void
	LANGUAGE plpgsql AS $$
	DECLARE
		v int;
		dist float8;
	BEGIN
		FOR i IN 1 .. maxrows LOOP
			FETCH cur INTO v, dist;
			EXIT WHEN NOT FOUND;
			INSERT INTO knn_shift_got (a, d) VALUES (v, dist);
		END LOOP;
	END $$;
}

teardown
{
	DROP TABLE knn_shift, knn_shift_wide, knn_shift_vac, knn_shift_got;
	DROP FUNCTION knn_shift_fetch(refcursor, int);
}

session s1
setup
{
	SET enable_seqscan = off;
	SET enable_bitmapscan = off;
	SET enable_sort = off;
	SET enable_indexonlyscan = off;
}
step s1begin	{ BEGIN ISOLATION LEVEL REPEATABLE READ; }
step s1beginrc	{ BEGIN ISOLATION LEVEL READ COMMITTED; }
step s1knn		{ DECLARE c CURSOR FOR SELECT a, a <~> 500 FROM knn_shift ORDER BY a <~> 500; }
step s1knnios
{
	SET LOCAL enable_indexonlyscan = on;
	DECLARE c CURSOR FOR SELECT a, a <~> 500 FROM knn_shift ORDER BY a <~> 500;
}
step s1wide		{ DECLARE c CURSOR FOR SELECT a, a <~> 15000 FROM knn_shift_wide ORDER BY a <~> 15000; }
step s1vac		{ DECLARE c CURSOR FOR SELECT a, a <~> 13000 FROM knn_shift_vac ORDER BY a <~> 13000; }
step s1fetch	{ SELECT knn_shift_fetch('c', 50); }
step s1fetchmany	{ SELECT knn_shift_fetch('c', 1000); }
step s1rest		{ SELECT knn_shift_fetch('c', 1000000); }
step s1commit	{ COMMIT; }
step s1count
{
	SELECT count(*) AS fetched, count(DISTINCT a) AS distinct_rows,
		   count(*) FILTER (WHERE a % 10 <> 0) AS foreign,
		   count(*) FILTER (WHERE d < prev_d) AS out_of_order
	  FROM (SELECT a, d, lag(d) OVER (ORDER BY n) AS prev_d
			  FROM knn_shift_got) s;
	TRUNCATE knn_shift_got;
}

session s2
step s2ins
{
	INSERT INTO knn_shift SELECT g FROM generate_series(1, 20) g;
	INSERT INTO knn_shift SELECT 500 + g FROM generate_series(1, 9) g;
}
step s2split	{ INSERT INTO knn_shift SELECT g * 10 + 5 FROM generate_series(1, 2000) g; }
step s2inswide	{ INSERT INTO knn_shift_wide SELECT g * 10 + 3 FROM generate_series(1, 3000) g; }
step s2del
{
	DELETE FROM knn_shift_vac
	 WHERE a BETWEEN 4000 AND 11990 OR a BETWEEN 14000 AND 21990;
}
step s2vacuum	{ VACUUM (INDEX_CLEANUP ON) knn_shift_vac; }

# Keys inserted below each cursor's position on the one leaf both cursors
# read: small keys on the backward side, keys just above the center on the
# forward side.  Plain and index-only scans.
permutation s1begin s1knn s1fetch s2ins s1rest s1commit s1count
permutation s1begin s1knnios s1fetch s2ins s1rest s1commit s1count

# The same while the inserts split the leaf, and the leaves after it, many
# times over on both sides of the center.
permutation s1begin s1knn s1fetch s2split s1rest s1commit s1count
permutation s1begin s1knnios s1fetch s2split s1rest s1commit s1count

# Both cursors already off the center leaf when inserts land on every leaf.
permutation s1begin s1wide s1fetchmany s2inswide s1rest s1commit s1count

# VACUUM deletes the emptied leaves on both sides of the cursors' leaf after
# the cursors saved links to them.  The forward cursor steps onto a deleted
# page and moves right past it; the backward cursor finds its saved left link
# deleted and recovers the live left sibling.  The rows were deleted before
# the cursor's snapshot, so VACUUM may remove them; a plain index scan holds
# no pin between rows, so VACUUM does not wait for the cursor.  1400 rows
# remain.
permutation s2del s1beginrc s1vac s1fetch s2vacuum s1rest s1commit s1count
