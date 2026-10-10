# Concurrent inserts into a unique BARK index with an extracted column
# ("When the check runs" in M4 of BARK-Design.mediawiki).  Each key of a row
# is checked and inserted under its leaf's lock, in ascending key order; a
# conflict with an in-progress row waits, then checks that key again.
#
# s1 inserts {1,5}; s2's {5,9} shares 5 and waits, then fails when s1
# commits, or succeeds when s1 aborts.  Rows with their keys in opposite
# order ({3,4} and {4,3}) insert their keys in the same ascending order, so
# they cannot wait on each other in a cycle.  The deleter wait: s2 deletes
# the row holding 7; s1 inserts {2,7}, inserting 2 and waiting on s2's
# delete for 7; s2 inserts {2}, which waits on s1's entry of 2.  The
# deadlock detector aborts s1 (its deadlock_timeout is the shorter) and s2
# commits.  After each permutation the index answers as a sequential scan
# and passes bark_index_check.

setup
{
	CREATE EXTENSION IF NOT EXISTS bark_multikey;
	CREATE EXTENSION IF NOT EXISTS amcheck;
	CREATE TABLE mku (id int, a int4[]);
	CREATE UNIQUE INDEX mku_a ON mku USING bark (a bark_int4_array_ops);
	INSERT INTO mku VALUES (0, ARRAY[7]);
}

teardown
{
	DROP TABLE mku;
}

session s1
setup		{ SET deadlock_timeout = '2s'; }
step s1_begin	{ BEGIN; }
step s1_ins15	{ INSERT INTO mku VALUES (1, ARRAY[1,5]); }
step s1_ins34	{ INSERT INTO mku VALUES (3, ARRAY[3,4]); }
step s1_ins27	{ INSERT INTO mku VALUES (2, ARRAY[2,7]); }
step s1_commit	{ COMMIT; }
step s1_abort	{ ROLLBACK; }
step s1_check
{
	SELECT id, a FROM mku ORDER BY id;
	SELECT count(*) AS entries FROM bark_multikey_entries('mku_a') WHERE NOT marker;
	SELECT bark_index_check('mku_a');
}

session s2
setup		{ SET deadlock_timeout = '100s'; }
step s2_begin	{ BEGIN; }
step s2_ins59	{ INSERT INTO mku VALUES (5, ARRAY[5,9]); }
step s2_ins43	{ INSERT INTO mku VALUES (4, ARRAY[4,3]); }
step s2_del7	{ DELETE FROM mku WHERE id = 0; }
step s2_ins2	{ INSERT INTO mku VALUES (6, ARRAY[2]); }
step s2_commit	{ COMMIT; }

# Shared key, inserter commits: the waiter fails.
permutation s1_begin s1_ins15 s2_ins59 s1_commit s1_check
# Shared key, inserter aborts: the waiter succeeds.
permutation s1_begin s1_ins15 s2_ins59 s1_abort s1_check
# Keys in opposite order: no deadlock, the second fails.
permutation s1_begin s1_ins34 s2_ins43 s1_commit s1_check
# Deleter wait: one aborts with a deadlock, the other commits.
permutation s2_begin s2_del7 s1_begin s1_ins27 s2_ins2 s1_abort s2_commit s1_check
