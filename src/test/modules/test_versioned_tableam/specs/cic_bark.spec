# CREATE INDEX CONCURRENTLY of a BARK index on heap_versioned, with
# concurrent updates that tag entries.
#
# The build waits for an advisory lock s1 holds, inside the index
# expression, so s1's updates run before the index is ready (the build's
# snapshot misses them; validation inserts their versions) and again once it
# is ready (they insert tagged entries themselves).  BARK's bulk delete
# hands validation (TID, tag) for every entry, and heap_versioned checks
# each pair against the tuple at the TID.  amcheck with heapallindexed then
# finds an entry for every live row.

setup
{
	CREATE EXTENSION test_versioned_tableam;
	CREATE EXTENSION amcheck;
	CREATE TABLE cb (id int PRIMARY KEY, k int, s text) USING heap_versioned;
	INSERT INTO cb SELECT g, g, 'abc' || g FROM generate_series(1, 200) g;
	CREATE FUNCTION cb_lock(int) RETURNS int IMMUTABLE LANGUAGE plpgsql AS $$
		BEGIN PERFORM pg_advisory_lock_shared(4244); PERFORM pg_advisory_unlock_shared(4244); RETURN $1; END;
	$$;
}

teardown
{
	DROP TABLE cb;
	DROP FUNCTION cb_lock(int);
	DROP EXTENSION amcheck;
	DROP EXTENSION test_versioned_tableam;
}

session s1
step s1_reset	{ SELECT test_versioned_reset(); }
step s1_lock	{ SELECT pg_advisory_lock(4244); }
step s1_upd		{ UPDATE cb SET k = k + 1000 WHERE id <= 50; }
step s1_unlock	{ SELECT pg_advisory_unlock(4244); }
step s1_upd2	{ UPDATE cb SET k = k + 1000 WHERE id > 150; }
step s1_check
{
	SELECT a.amname, x.indisready, x.indisvalid
	  FROM pg_index x JOIN pg_class c ON c.oid = x.indexrelid JOIN pg_am a ON a.oid = c.relam
	 WHERE x.indexrelid = 'cb_k'::regclass;
	SELECT bark_index_parent_check('cb_k', true);
	SET enable_seqscan = off;
	SET enable_bitmapscan = off;
	SELECT count(*), sum(k) FROM cb WHERE cb_lock(k) > 0;
	RESET enable_seqscan;
	RESET enable_bitmapscan;
	SELECT n > 0 AS validated_tagged FROM test_versioned_counts() WHERE counter = 'validate_tagged';
	SELECT n > 0 AS inserted_tagged FROM test_versioned_counts() WHERE counter = 'insert_tagged';
}

session s2
step s2_cic		{ CREATE INDEX CONCURRENTLY cb_k ON cb USING bark (cb_lock(k)); }

permutation s1_reset s1_lock s2_cic s1_upd s1_unlock s1_upd2 s1_check
