# The meta page's conversion bits changed by another backend between a
# caller's unlocked read and bark_change_conversion_flags' exclusive lock.
#
# tcr_doc stores tags on its keys (MKTAGS) and has just been made an
# exclusion constraint's, so the first BARK call to see that sets
# BARK_META_CONVERTING (8).  s1 reads the bits, finds MKTAGS (4) alone, and
# stops at bark-conversion-flags-before-lock, before it locks the meta page.
#
# 1. s1 is an insert and s2 another insert, which sets CONVERTING.  s1 then
#    finds the bit already set and changes nothing.
# 2. s1 is a constraint check's dirty scan, which holds only AccessShareLock
#    on the table, so s2's VACUUM, whose barrier waits out locks that
#    conflict with ShareLock, does not wait for it: it sets CONVERTING and
#    SETTLED (16), leaves no tagged entry, and clears all three, which
#    leaves MULTIKEY (2).  s1 then finds MKTAGS gone and changes nothing;
#    its scan returns row 1.  A writer cannot see this, since the barrier
#    waits out its RowExclusiveLock, and so a writer that read MKTAGS alone
#    has set CONVERTING before any VACUUM can complete the conversion.
# s1_done, an empty step of s1, comes after the wakeup so that s1's step is
# reported done before s3_check runs.

setup
{
	CREATE EXTENSION test_versioned_tableam;
	CREATE EXTENSION bark_multikey;
	CREATE EXTENSION injection_points;
	CREATE EXTENSION amcheck;
	CREATE EXTENSION pageinspect;
}

setup
{
	CREATE TABLE tcr (id int, doc int4[]) USING heap_versioned
	  WITH (autovacuum_enabled = off);
	INSERT INTO tcr VALUES (1, ARRAY[1,2]), (2, ARRAY[20,21]);
	CREATE INDEX tcr_doc ON tcr USING bark (doc bark_int4_array_ops);
	SELECT test_versioned_add_exclusion('tcr_doc', '=(anyarray,anyarray)');
	UPDATE pg_catalog.pg_index SET indisexclusion = true
	  WHERE indexrelid = 'tcr_doc'::regclass;
}

teardown
{
	DROP TABLE tcr;
	DROP EXTENSION pageinspect;
	DROP EXTENSION amcheck;
	DROP EXTENSION injection_points;
	DROP EXTENSION bark_multikey;
	DROP EXTENSION test_versioned_tableam;
}

session s1
setup
{
	SELECT injection_points_set_local();
	SELECT injection_points_attach('bark-conversion-flags-before-lock', 'wait');
}
step s1_insert		{ INSERT INTO tcr VALUES (3, ARRAY[5,6]); }
step s1_scan
{
	SELECT locator FROM test_versioned_dirty_scan('tcr_doc',
	  '&&(anyarray,anyarray)', ARRAY[1,2]);
}
step s1_done		{ }
teardown	{ SELECT injection_points_detach('bark-conversion-flags-before-lock'); }

session s2
step s2_insert		{ INSERT INTO tcr VALUES (4, ARRAY[7,8]); }
step s2_vacuum		{ VACUUM tcr; }
step s2_bits		{ SELECT flags & 30 AS mktags_converting_settled FROM bark_metap('tcr_doc'); }

session s3
step s3_wakeup		{ SELECT injection_points_wakeup('bark-conversion-flags-before-lock'); }
step s3_check
{
	SELECT flags & 30 AS mktags_converting_settled FROM bark_metap('tcr_doc');
	SELECT id, doc FROM tcr ORDER BY id;
	SELECT bark_index_check('tcr_doc');
}

permutation s1_insert s2_insert s2_bits s3_wakeup s1_done s3_check
permutation s1_scan s2_vacuum s2_bits s3_wakeup s1_done s3_check
