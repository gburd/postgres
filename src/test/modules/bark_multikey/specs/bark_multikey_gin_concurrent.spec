# A scan through GIN's query interface that concurrent sessions change
# under.
#
# The scan reads each boundary of its key into a bitmap of its own, one
# after the other, with no pin held between them, and merges the bitmaps
# at the end: no stream keeps a leaf position while another is read.  s1
# stops in the descent of its third boundary (bark-search-descend, which
# every boundary's read passes once in a two-level index) after reading two;
# s2 then inserts rows whose keys land on the leaves s1 has read and has yet
# to read, splitting them, deletes some rows and vacuums.  s1's snapshot
# predates s2's work, so its rows must be exactly a sequential scan's in the
# same snapshot: the bitmap scan's whatever the index now holds, since the
# heap decides visibility, and the index scan's too.  @> makes the merge
# combine streams (not the direct union), and the second key a two-key AND.

setup
{
	CREATE EXTENSION IF NOT EXISTS bark_multikey;
	CREATE EXTENSION IF NOT EXISTS amcheck;
	CREATE EXTENSION IF NOT EXISTS injection_points;
	CREATE EXTENSION IF NOT EXISTS pageinspect;
	CREATE TABLE gbc (id int, a int4[]) WITH (autovacuum_enabled = off);
	INSERT INTO gbc SELECT g, ARRAY[g % 7, 100 + g % 13, 200 + g % 5]
	  FROM generate_series(1, 6000) g;
	-- SINGLE entries only, so that the index has many leaves and every
	-- boundary's read descends from the root.
	CREATE INDEX gbc_a ON gbc USING bark (a bark_gin_int4_array_ops)
	  WITH (deduplicate_items = off);
	CREATE TABLE gbc_got (id int);
}

teardown
{
	DROP TABLE gbc, gbc_got;
}

session s1
setup
{
	SELECT injection_points_set_local();
	SET enable_seqscan = off;
}
step s1_begin	{ BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT count(*) > 0 AS snap FROM gbc; }
# The descent of the third boundary read waits: two have been read.
step s1_attach
{
	SELECT injection_points_attach('bark-search-descend', 'wait');
}
step s1_bitmap
{
	SET LOCAL enable_indexscan = off;
	SET LOCAL enable_bitmapscan = on;
	INSERT INTO gbc_got SELECT id FROM gbc WHERE a @> ARRAY[3,103,203];
}
step s1_index
{
	SET LOCAL enable_indexscan = on;
	SET LOCAL enable_bitmapscan = off;
	INSERT INTO gbc_got SELECT id FROM gbc WHERE a @> ARRAY[3,103] AND a @> ARRAY[203];
}
step s1_cmp_bitmap
{
	SET LOCAL enable_seqscan = on;
	SET LOCAL enable_indexscan = off;
	SET LOCAL enable_bitmapscan = off;
	SELECT (SELECT count(*) || ':' || count(DISTINCT id) || ':' || sum(id) FROM gbc_got) AS scan,
	       (SELECT count(*) || ':' || count(DISTINCT id) || ':' || sum(id)
	          FROM gbc WHERE a @> ARRAY[3,103,203]) AS seqscan;
}
step s1_cmp_index
{
	SET LOCAL enable_seqscan = on;
	SET LOCAL enable_indexscan = off;
	SET LOCAL enable_bitmapscan = off;
	SELECT (SELECT count(*) || ':' || count(DISTINCT id) || ':' || sum(id) FROM gbc_got) AS scan,
	       (SELECT count(*) || ':' || count(DISTINCT id) || ':' || sum(id)
	          FROM gbc WHERE a @> ARRAY[3,103] AND a @> ARRAY[203]) AS seqscan;
}
step s1_commit	{ COMMIT; }

session s2
setup
{
	SELECT injection_points_set_local();
}
# Many new rows on every key's leaves (splits), the scan's keys among them,
# and deletes of rows the scan matches.
step s2_churn
{
	INSERT INTO gbc SELECT g, ARRAY[g % 7, 100 + g % 13, 200 + g % 5]
	  FROM generate_series(10001, 22000) g;
	DELETE FROM gbc WHERE id % 4 = 0 AND id <= 6000;
}
step s2_vacuum	{ VACUUM gbc; }
step s2_check
{
	SELECT bark_index_check('gbc_a') AS gbc_a,
	       pg_relation_size('gbc_a') > 8192 * 20 AS grew;
}
# Before the churn the index has a root above its leaves, so each of the
# scan's boundary reads descends past bark-search-descend once, and the
# churn splits leaves the scan has read and leaves it has not.
step s2_shape
{
	SELECT (bark_metap('gbc_a')).level >= 1 AS has_root,
	       pg_relation_size('gbc_a') / 8192 AS pages_before;
}

session s3
# Let one waiting descent go on; s1 reads that boundary and waits again.
step s3_wake1	{ SELECT injection_points_wakeup('bark-search-descend'); }
step s3_wake
{
	SELECT injection_points_detach('bark-search-descend');
	SELECT injection_points_wakeup('bark-search-descend');
}

permutation s2_shape s1_begin s1_attach s1_bitmap s3_wake1 s3_wake1 s2_churn s2_vacuum
	s3_wake s1_cmp_bitmap s1_commit s2_check
permutation s1_begin s1_attach s1_index s3_wake1 s3_wake1 s2_churn s2_vacuum
	s3_wake s1_cmp_index s1_commit s2_check
