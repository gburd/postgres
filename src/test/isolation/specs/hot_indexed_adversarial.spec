# Adversarial correctness tests for HOT-indexed (SIU) updates.
#
# Each permutation pins a case that the mid-chain-pointing invariant must
# satisfy: an index entry points at the heap-only version whose key it
# matched, and a chain walk that crosses a HOT-indexed hop is rechecked
# against the live tuple's current key (the bitmap-overlap test).  These are
# exactly the cases that historically broke write-amplification-reduction
# designs.

setup
{
    CREATE TABLE hia (id int PRIMARY KEY, k int, pad text) WITH (fillfactor = 40);
    CREATE INDEX hia_k ON hia(k);
    CREATE TABLE hiu (id int PRIMARY KEY, k int, pad text) WITH (fillfactor = 40);
    CREATE UNIQUE INDEX hiu_k ON hiu(k);
    INSERT INTO hia VALUES (1, 10, repeat('x', 40));
    INSERT INTO hiu VALUES (1, 10, repeat('x', 40));
}

teardown
{
    DROP TABLE hia;
    DROP TABLE hiu;
}

session s1
step s1_begin   { BEGIN; }
# Cycle the indexed key away and back: 10 -> 20 -> 10.  The original 10 leaf
# and the freshly-inserted 10 leaf both resolve to the live tuple; the chain
# walk must drop the stale one so a lookup returns the row exactly once.
step s1_cycle   { UPDATE hia SET k = 20 WHERE id = 1; UPDATE hia SET k = 10 WHERE id = 1; }
# A single HOT-indexed update on hia, used by the abort and reader cases.
step s1_upd20   { UPDATE hia SET k = 20 WHERE id = 1; }
# A HOT-indexed update on the UNIQUE-indexed table, freeing key 10 for k=20.
step s1_uupd20  { UPDATE hiu SET k = 20 WHERE id = 1; }
step s1_commit  { COMMIT; }
step s1_abort   { ROLLBACK; }

session s2
# Index-only lookups (forced) that exercise the stale-leaf recheck.
step s2_noseq   { SET enable_seqscan = off; }
step s2_eq10    { SELECT id, k FROM hia WHERE k = 10; }
step s2_eq20    { SELECT id, k FROM hia WHERE k = 20; }
step s2_range   { SELECT id, k FROM hia WHERE k >= 5 ORDER BY k; }
# Concurrent unique insert of the key s1 is freeing (10) and of the key s1 is
# taking (20); _bt_check_unique must filter the stale 10 leaf but still
# conflict on the live key.
step s2_ins10   { INSERT INTO hiu VALUES (2, 10, repeat('y', 40)); }
step s2_ins20   { INSERT INTO hiu VALUES (3, 20, repeat('z', 40)); }

# 1. a->b->a cycle: exactly one row for the cycled-back key, none for the
#    transient key.
permutation s2_noseq s1_cycle s2_eq10 s2_eq20

# 2. Range scan returns the live row exactly once across the stale+fresh
#    leaves left by the cycle.
permutation s2_noseq s1_cycle s2_range

# 3. Abort of a HOT-indexed update: the new key must not surface and the old
#    key must still resolve to the (restored) live tuple.
permutation s2_noseq s1_begin s1_upd20 s1_abort s2_eq20 s2_eq10

# 4. Concurrent unique insert while a HOT-indexed update is in flight.  An
#    insert of the key s1 is freeing (10) must wait on the uncommitted updater,
#    then succeed once it commits (the stale 10 leaf is filtered).
permutation s1_begin s1_uupd20 s2_ins10 s1_commit

# 5. After the update commits, the freed key (10) inserts cleanly and the
#    taken key (20) conflicts -- the live leaf is honoured, the stale one is not.
permutation s1_begin s1_uupd20 s1_commit s2_ins10
permutation s1_begin s1_uupd20 s1_commit s2_ins20
