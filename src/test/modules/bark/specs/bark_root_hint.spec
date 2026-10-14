# The root buffer hint across another backend's root split.
#
# s1 warms its root cache (BarkAmCache.root, level and rootbuf) with a few
# lookups.  s2 then inserts until the root splits, so s1's cached root is no
# longer the root: on its next descent the page check refuses it (the
# bark-root-cache-stale notice), the meta page is read, and the new root's
# buffer becomes the hint, which the descents after that use silently.  s2
# finally evicts the new root's buffer, so s1's hint names a buffer that no
# longer holds the root block (bark-root-hint-miss), and s1 reads it again.
# Every lookup must return its row.

setup
{
	CREATE EXTENSION injection_points;
	CREATE EXTENSION pg_buffercache;
	CREATE EXTENSION pageinspect;
	CREATE TABLE rh (i int4) WITH (autovacuum_enabled = off);
	CREATE INDEX rh_idx ON rh USING bark (i);
	INSERT INTO rh SELECT g FROM generate_series(1, 20000) g;
}

teardown
{
	DROP TABLE rh;
	DROP EXTENSION injection_points;
	DROP EXTENSION pg_buffercache;
	DROP EXTENSION pageinspect;
}

session s1
setup
{
	SET enable_seqscan = off;
	SET enable_bitmapscan = off;
	SELECT injection_points_set_local();
	SELECT injection_points_attach('bark-root-hint-miss', 'notice');
	SELECT injection_points_attach('bark-root-cache-stale', 'notice');
}
step s1_warm	{ SELECT count(*) FROM rh WHERE i IN (17, 4242, 19999); }
step s1_look	{ SELECT count(*) FROM rh WHERE i = 4242; }
step s1_look2	{ SELECT count(*) FROM rh WHERE i = 350000; }
step s1_level	{ SELECT level FROM bark_metap('rh_idx'); }

session s2
step s2_split	{ INSERT INTO rh SELECT g FROM generate_series(20001, 400000) g; }
step s2_evict
{
	SELECT bool_and((pg_buffercache_evict(b.bufferid)).buffer_evicted) AS evicted
	FROM pg_buffercache b
	WHERE b.relfilenode = pg_relation_filenode('rh_idx')
	  AND b.relblocknumber = (SELECT root FROM bark_metap('rh_idx'));
}

permutation s1_warm s1_level s2_split s1_level s1_look s1_look2 s1_look s2_evict s1_look s1_look2
