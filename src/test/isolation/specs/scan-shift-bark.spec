# A BARK scan reads a leaf page at a time: it copies the page's matching
# heap TIDs when it first reads the page, then returns them from that copy.
# Concurrent inserts and splits on the page cannot make it skip or repeat
# rows, and VACUUM is not held up by a plain index scan's cursor.
#
# The base code resumed by page offset with the lock released between rows,
# so an insert that shifted the page's items made a cursor return some rows
# twice (repro: 118 rows, 18 duplicates for 100).

setup
{
	CREATE TABLE bark_shift (a int, pad text) WITH (autovacuum_enabled = off);
	INSERT INTO bark_shift SELECT g * 10, 'x' FROM generate_series(1, 100) g;
	CREATE INDEX bark_shift_idx ON bark_shift USING bark (a);
	CREATE TABLE bark_shift_list (a int) WITH (autovacuum_enabled = off);
	INSERT INTO bark_shift_list SELECT g / 50 FROM generate_series(0, 299) g;
	CREATE INDEX bark_shift_list_idx ON bark_shift_list USING bark (a);
	CREATE TABLE bark_shift_vac (a int, pad text) WITH (autovacuum_enabled = off);
	INSERT INTO bark_shift_vac SELECT g, repeat('y', 100) FROM generate_series(1, 5000) g;
	CREATE INDEX bark_shift_vac_idx ON bark_shift_vac USING bark (a);
	ANALYZE bark_shift, bark_shift_list, bark_shift_vac;
	CREATE TABLE bark_shift_got (a int);
	CREATE FUNCTION bark_shift_fetch(cur refcursor, maxrows int) RETURNS void
	LANGUAGE plpgsql AS $$
	DECLARE
		v int;
	BEGIN
		FOR i IN 1 .. maxrows LOOP
			FETCH cur INTO v;
			EXIT WHEN NOT FOUND;
			INSERT INTO bark_shift_got VALUES (v);
		END LOOP;
	END $$;
}

teardown
{
	DROP TABLE bark_shift, bark_shift_list, bark_shift_vac, bark_shift_got;
	DROP FUNCTION bark_shift_fetch(refcursor, int);
}

session s1
setup
{
	SET enable_seqscan = off;
	SET enable_bitmapscan = off;
	SET enable_indexonlyscan = off;
}
step s1begin	{ BEGIN ISOLATION LEVEL REPEATABLE READ; }
step s1beginrc	{ BEGIN ISOLATION LEVEL READ COMMITTED; }
step s1fwd		{ DECLARE c CURSOR FOR SELECT a FROM bark_shift WHERE a > 0 ORDER BY a; }
step s1bwd		{ DECLARE c CURSOR FOR SELECT a FROM bark_shift WHERE a > 0 ORDER BY a DESC; }
step s1list		{ DECLARE c CURSOR FOR SELECT a FROM bark_shift_list WHERE a >= 0 ORDER BY a; }
step s1vac		{ DECLARE c CURSOR FOR SELECT a FROM bark_shift_vac WHERE a > 0 ORDER BY a; }
step s1fetch	{ SELECT bark_shift_fetch('c', 50); }
step s1fetch1	{ SELECT bark_shift_fetch('c', 1); }
step s1rest		{ SELECT bark_shift_fetch('c', 1000000); }
step s1commit	{ COMMIT; }
step s1count
{
	SELECT count(*) AS fetched, count(DISTINCT a) AS distinct_rows,
		   min(a), max(a) FROM bark_shift_got;
	TRUNCATE bark_shift_got;
}

session s2
step s2ins		{ INSERT INTO bark_shift SELECT g FROM generate_series(1, 20) g; }
step s2split	{ INSERT INTO bark_shift SELECT g * 10 + 5, 'z' FROM generate_series(1, 2000) g; }
step s2inslist	{ INSERT INTO bark_shift_list SELECT 0 FROM generate_series(1, 30); }
step s2delhalf	{ DELETE FROM bark_shift_vac WHERE a % 2 = 0; }
step s2vacuum	{ VACUUM (INDEX_CLEANUP ON) bark_shift_vac; }

# Smaller keys inserted onto the leaf the cursor is reading, forward and
# backward: the rest of the scan returns exactly the rows the snapshot sees.
permutation s1begin s1fwd s1fetch s2ins s1rest s1commit s1count
permutation s1begin s1bwd s1fetch s2ins s1rest s1commit s1count

# The same while the inserts split the leaf, and the leaves after it, many
# times over.
permutation s1begin s1fwd s1fetch s2split s1rest s1commit s1count
permutation s1begin s1bwd s1fetch s2split s1rest s1commit s1count

# Stopped inside one LIST entry's members, while that entry grows.
permutation s1begin s1list s1fetch1 s2inslist s1rest s1commit s1count

# A plain index scan with an MVCC snapshot holds no pin on its leaf between
# rows, so VACUUM does not wait for the open cursor.  Every other row is
# deleted before the cursor takes its snapshot, so they are removable and
# every leaf, the cursor's included, has dead entries VACUUM must remove
# under a cleanup lock.  If the cursor kept its pin, s2vacuum would wait for
# it forever (isolationtester cannot see a buffer-pin wait).  An index-only
# scan does keep its pin; that is not testable here for the same reason.
permutation s2delhalf s1beginrc s1vac s1fetch s2vacuum s1rest s1commit s1count
