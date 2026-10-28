# bark_reclaim_orphans against a concurrent insert of an oversized key.
#
# The first permutation stops an insert between writing its key's overflow
# chain and placing the entry (bark-overflow-chain-written): its chain is
# referenced by nothing yet.  The reclaim waits for the insert's lock on the
# index and, once the insert is done, finds the new chain referenced and
# frees nothing.
#
# The second permutation stops the reclaim after its census, holding
# ShareRowExclusiveLock (bark-reclaim-orphans-counted), with an orphan left
# by an insert that failed between writing its chain and placing its entry.
# A scan and bark_index_check run meanwhile; an oversized insert waits for
# the reclaim, which frees the orphan alone.  Afterwards the parent check
# reports no orphan and the index answers as a sequential scan does.
#
# The third permutation asks for another session's temporary index, whose
# pages are in that session's local buffers, and is refused.

setup
{
	CREATE EXTENSION IF NOT EXISTS injection_points;
	CREATE EXTENSION IF NOT EXISTS amcheck;
	CREATE TABLE bark_ro (id int, k text);
	CREATE INDEX bark_ro_idx ON bark_ro USING bark (k);
	CREATE FUNCTION bark_ro_big(s int) RETURNS text LANGUAGE sql IMMUTABLE AS
	$$ SELECT string_agg(md5(s::text || ':' || g::text), '')
	   FROM generate_series(1, 640) g $$;
	INSERT INTO bark_ro SELECT g, bark_ro_big(g) FROM generate_series(1, 10) g;
	-- The index-only scan's and a sequential scan's keys agree.
	CREATE FUNCTION bark_ro_same() RETURNS text LANGUAGE plpgsql AS $$
	DECLARE a text; b text;
	BEGIN
	  SET LOCAL enable_seqscan = off; SET LOCAL enable_bitmapscan = off;
	  SELECT string_agg(md5(k), ',' ORDER BY k) INTO a FROM bark_ro WHERE k > '';
	  SET LOCAL enable_seqscan = on; SET LOCAL enable_indexscan = off;
	  SET LOCAL enable_indexonlyscan = off;
	  SELECT string_agg(md5(k), ',' ORDER BY k) INTO b FROM bark_ro;
	  RETURN format('%s rows, %s', (SELECT count(*) FROM bark_ro),
	                CASE WHEN a = b THEN 'same' ELSE 'differ' END);
	END $$;
}

teardown
{
	DROP TABLE bark_ro;
	DROP FUNCTION bark_ro_big(int), bark_ro_same();
	DROP EXTENSION amcheck;
	DROP EXTENSION injection_points;
}

session s1
setup
{
	SELECT injection_points_set_local();
}
step s1_attach_written
{
	SELECT injection_points_attach('bark-overflow-chain-written', 'wait');
}
step s1_insert	{ INSERT INTO bark_ro VALUES (100, bark_ro_big(100)); }
step s1_orphan
{
	SELECT injection_points_attach('bark-overflow-chain-written', 'error');
	INSERT INTO bark_ro VALUES (200, bark_ro_big(200));
}
step s1_detach
{
	SELECT injection_points_detach('bark-overflow-chain-written');
}
step s1_temp
{
	CREATE TEMP TABLE bark_ro_tmp (k text);
	CREATE INDEX bark_ro_tmp_idx ON bark_ro_tmp USING bark (k);
}
step s1_check
{
	SELECT bark_index_parent_check('bark_ro_idx');
	SELECT bark_ro_same();
}

session s2
setup
{
	SELECT injection_points_set_local();
}
step s2_attach_counted
{
	SELECT injection_points_attach('bark-reclaim-orphans-counted', 'wait');
}
step s2_reclaim	{ SELECT bark_reclaim_orphans('bark_ro_idx') AS freed; }
step s2_reclaim_temp
{
	SELECT bark_reclaim_orphans(c.oid)
	  FROM pg_class c WHERE c.relname = 'bark_ro_tmp_idx';
}

session s3
step s3_wakeup_written
{
	SELECT injection_points_wakeup('bark-overflow-chain-written');
}
step s3_scan
{
	SET enable_seqscan = off; SET enable_bitmapscan = off;
	SELECT count(*) AS index_count FROM bark_ro WHERE k > '';
	SELECT bark_index_check('bark_ro_idx');
}
step s3_insert	{ INSERT INTO bark_ro VALUES (300, bark_ro_big(300)); }
# s1_check from s3: it runs once s3_insert is done, so it never waits.
step s3_check
{
	SELECT bark_index_parent_check('bark_ro_idx');
	SELECT bark_ro_same();
}

session s4
step s4_wakeup_counted
{
	SELECT injection_points_detach('bark-reclaim-orphans-counted');
	SELECT injection_points_wakeup('bark-reclaim-orphans-counted');
}

# s2_reclaim waits for s1_insert's lock; once woken, s1_insert finishes, and
# then s2_reclaim, which frees nothing.
permutation s1_attach_written s1_insert s2_reclaim(s1_insert) s3_wakeup_written s1_detach s1_check

# s3_insert waits for s2_reclaim's lock; s2_reclaim frees the orphan s1_orphan
# left, then s3_insert finishes, and s3_check runs.
permutation s1_orphan s1_detach s2_attach_counted s2_reclaim s3_scan s3_insert(s2_reclaim) s4_wakeup_counted s3_check

# Another session's temporary index cannot be read here.
permutation s1_temp s2_reclaim_temp
