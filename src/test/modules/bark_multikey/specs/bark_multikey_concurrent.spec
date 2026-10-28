# A multikey scan that a concurrent session splits under.
#
# Session s1 opens a cursor over an index scan of a multikey index and reads
# part of it.  Session s2 then inserts rows whose keys land on the leaves the
# scan has read and has yet to read, splitting them, deletes some of the
# rows s1 has not reached, and vacuums.  s1 reads the rest.  Its snapshot
# predates s2's work, so the cursor must return exactly the rows a
# sequential scan in the same snapshot returns, each once: the scan copies a
# leaf's matches when it reads the leaf, moves on by the right links it
# saved, re-descends to its next boundary from the root, and drops a row's
# later entries through its seen set.  mks1's index has the extracted column
# alone (the boundaries filter); mks2's has a fixed column first (the
# boundaries position the scan, which re-descends between them).

setup
{
	CREATE EXTENSION bark_multikey;
	CREATE EXTENSION amcheck;
	CREATE TABLE mks1 (id int, tenant int, a int4[]) WITH (autovacuum_enabled = off);
	INSERT INTO mks1 SELECT g, g % 3,
		ARRAY(SELECT (g * 7 + i * 31) % 200 FROM generate_series(1, 1 + g % 8) i)
	  FROM generate_series(1, 6000) g;
	CREATE TABLE mks2 WITH (autovacuum_enabled = off) AS SELECT * FROM mks1;
	CREATE INDEX mks1_a ON mks1 USING bark (a bark_int4_array_ops);
	CREATE INDEX mks2_ta ON mks2 USING bark (tenant, a bark_int4_array_ops);
	CREATE TABLE mks_got (id int);

	-- Fetch up to n rows (all when n is NULL) of cursor cur into mks_got.
	CREATE FUNCTION mks_fetch(cur refcursor, n int) RETURNS void
	LANGUAGE plpgsql AS $$
	DECLARE
		v int;
		got int := 0;
	BEGIN
		WHILE n IS NULL OR got < n LOOP
			FETCH cur INTO v;
			EXIT WHEN NOT FOUND;
			INSERT INTO mks_got VALUES (v);
			got := got + 1;
		END LOOP;
	END $$;
}

teardown
{
	DROP TABLE mks1, mks2, mks_got;
	DROP FUNCTION mks_fetch(refcursor, int);
	DROP EXTENSION amcheck;
	DROP EXTENSION bark_multikey;
}

session s1
setup
{
	SET enable_seqscan = off;
	SET enable_bitmapscan = off;
	SET enable_indexonlyscan = off;
}
step s1_begin	{ BEGIN ISOLATION LEVEL REPEATABLE READ; }
step s1_open1
{
	DECLARE c NO SCROLL CURSOR FOR
		SELECT id FROM mks1 WHERE a && ARRAY[3,50,77,120,199];
}
step s1_open2
{
	DECLARE c NO SCROLL CURSOR FOR
		SELECT id FROM mks2 WHERE tenant IN (0, 2) AND a && ARRAY[3,50,77,120,199];
}
step s1_fetch	{ SELECT mks_fetch('c', 150); }
step s1_rest	{ SELECT mks_fetch('c', NULL); }
step s1_cmp1
{
	SET LOCAL enable_seqscan = on;
	SET LOCAL enable_indexscan = off;
	SELECT (SELECT count(*) || ':' || count(DISTINCT id) || ':' || sum(id) FROM mks_got) AS cursor,
	       (SELECT count(*) || ':' || count(DISTINCT id) || ':' || sum(id)
	          FROM mks1 WHERE a && ARRAY[3,50,77,120,199]) AS seqscan;
}
step s1_cmp2
{
	SET LOCAL enable_seqscan = on;
	SET LOCAL enable_indexscan = off;
	SELECT (SELECT count(*) || ':' || count(DISTINCT id) || ':' || sum(id) FROM mks_got) AS cursor,
	       (SELECT count(*) || ':' || count(DISTINCT id) || ':' || sum(id)
	          FROM mks2 WHERE tenant IN (0, 2) AND a && ARRAY[3,50,77,120,199]) AS seqscan;
}
step s1_commit	{ COMMIT; }

session s2
step s2_churn1
{
	INSERT INTO mks1 SELECT g, g % 3,
		ARRAY(SELECT (g * 11 + i * 17) % 200 FROM generate_series(1, 10) i)
	  FROM generate_series(10001, 16000) g;
	DELETE FROM mks1 WHERE id % 5 = 0 AND id > 3000 AND id <= 6000;
}
step s2_churn2
{
	INSERT INTO mks2 SELECT g, g % 3,
		ARRAY(SELECT (g * 11 + i * 17) % 200 FROM generate_series(1, 10) i)
	  FROM generate_series(10001, 16000) g;
	DELETE FROM mks2 WHERE id % 5 = 0 AND id > 3000 AND id <= 6000;
}
step s2_vacuum1	{ VACUUM mks1; }
step s2_vacuum2	{ VACUUM mks2; }
step s2_check
{
	SELECT bark_index_check('mks1_a') AS mks1_a, bark_index_check('mks2_ta') AS mks2_ta,
	       pg_relation_size('mks1_a') > 8192 * 40 AS mks1_a_grew,
	       pg_relation_size('mks2_ta') > 8192 * 40 AS mks2_ta_grew;
}

permutation s1_begin s1_open1 s1_fetch s2_churn1 s2_vacuum1 s1_rest s1_cmp1 s1_commit s2_check
permutation s1_begin s1_open2 s1_fetch s2_churn2 s2_vacuum2 s1_rest s1_cmp2 s1_commit s2_check
