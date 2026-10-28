# Concurrent writers under an exclusion constraint on a BARK index's
# extracted column.  An insert puts in its entries, then scans for
# conflicts with a dirty snapshot, so a conflicting row whose writer is in
# progress makes the check wait for it, then scan again.
#
# s1 inserts {1,5}; s2's {5,9} shares 5 and waits, then fails when s1
# commits, or succeeds when s1 aborts.  Under the deferrable constraint s2
# does not wait at its insert; its check at commit waits for s1, then fails
# or succeeds.  ON CONFLICT DO NOTHING: s2's {12,13} shares 12 with s1's
# uncommitted {11,12}; it waits, then inserts nothing.  A HOT update to the
# same keys keeps the old entries: an insert sharing a key reaches both
# versions through them, waits for the update, and fails whether it commits
# or aborts.  After each permutation the indexes pass bark_index_check.

setup
{
	CREATE EXTENSION IF NOT EXISTS bark_multikey;
	CREATE EXTENSION IF NOT EXISTS amcheck;
	CREATE TABLE mkx (id int, a int4[],
		CONSTRAINT mkx_a EXCLUDE USING bark (a bark_int4_array_ops WITH |&&|))
		WITH (fillfactor = 10, autovacuum_enabled = off);
	CREATE TABLE mkxd (id int, a int4[],
		CONSTRAINT mkxd_a EXCLUDE USING bark (a bark_int4_array_ops WITH |&&|)
			DEFERRABLE INITIALLY DEFERRED);
	INSERT INTO mkx VALUES (0, ARRAY[20, 21]);
}

teardown
{
	DROP TABLE mkx, mkxd;
}

session s1
step s1_begin	{ BEGIN; }
step s1_ins15	{ INSERT INTO mkx VALUES (1, ARRAY[1,5]); }
step s1_insd	{ INSERT INTO mkxd VALUES (1, ARRAY[1,5]); }
step s1_oc		{ INSERT INTO mkx VALUES (11, ARRAY[11,12]) ON CONFLICT DO NOTHING; }
step s1_hot		{ UPDATE mkx SET a = ARRAY[21, 20, 20] WHERE id = 0; }
step s1_hotcount	{ SELECT pg_stat_get_xact_tuples_hot_updated('mkx'::regclass) AS hot; }
step s1_commit	{ COMMIT; }
step s1_abort	{ ROLLBACK; }
step s1_check
{
	SELECT id, a FROM mkx ORDER BY id;
	SELECT id, a FROM mkxd ORDER BY id;
	SELECT bark_index_check('mkx_a', true), bark_index_check('mkxd_a', true);
}

session s2
step s2_begin	{ BEGIN; }
step s2_ins59	{ INSERT INTO mkx VALUES (5, ARRAY[5,9]); }
step s2_insd	{ INSERT INTO mkxd VALUES (5, ARRAY[5,9]); }
step s2_oc		{ INSERT INTO mkx VALUES (12, ARRAY[12,13]) ON CONFLICT DO NOTHING; }
step s2_ins21	{ INSERT INTO mkx VALUES (2, ARRAY[21]); }
step s2_commit	{ COMMIT; }

# Shared key, inserter commits: the waiter fails.
permutation s1_begin s1_ins15 s2_ins59 s1_commit s1_check
# Shared key, inserter aborts: the waiter succeeds.
permutation s1_begin s1_ins15 s2_ins59 s1_abort s1_check
# Deferrable: the check at commit waits, then fails ...
permutation s1_begin s1_insd s2_begin s2_insd s2_commit s1_commit s1_check
# ... or succeeds.
permutation s1_begin s1_insd s2_begin s2_insd s2_commit s1_abort s1_check
# ON CONFLICT DO NOTHING sharing a key: the second waits, then does nothing.
permutation s1_begin s1_oc s2_oc s1_commit s1_check
# HOT update to the same keys: the insert waits, then fails ...
permutation s1_begin s1_hot s1_hotcount s2_ins21 s1_commit s1_check
# ... against the old version if the update aborts.
permutation s1_begin s1_hot s2_ins21 s1_abort s1_check
