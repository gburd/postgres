# Concurrency / MVCC correctness for the bm25 index access method.
#
# Exercises the paths the plain regression test cannot: two backends
# interleaving DML, VACUUM (which flushes the pending list and merges
# segments), and @@@ scans, under the isolation tester's serialized
# permutations.  Invariants:
#   * a REPEATABLE READ scan never sees another session's row committed after
#     the snapshot was taken (snapshot stability);
#   * an INSERT committed before a NEW scan IS seen (pending list is live);
#   * a concurrent VACUUM / fts_merge does not change what an open snapshot's
#     scan returns (merge is a physical, MVCC-invisible reorganization);
#   * delete + VACUUM + heap-slot reuse neither resurrects the deleted doc nor
#     hides the reusing doc (per-segment tombstones).

setup
{
    CREATE EXTENSION IF NOT EXISTS pg_fts;
    CREATE TABLE tdoc (id int primary key, d ftsdoc);
    INSERT INTO tdoc SELECT g, to_ftsdoc('alpha doc ' || g)
        FROM generate_series(1, 200) g;
    CREATE INDEX tdoc_bm25 ON tdoc USING bm25 (d);
}

teardown
{
    DROP TABLE tdoc;
}

# Reader that holds an explicit REPEATABLE READ snapshot.
session "r"
setup           { SET enable_seqscan = off; }
step "r_begin"  { BEGIN ISOLATION LEVEL REPEATABLE READ; }
step "r_alpha"  { SELECT count(*) AS alpha FROM tdoc WHERE d @@@ 'alpha'::ftsquery; }
step "r_beta"   { SELECT count(*) AS beta  FROM tdoc WHERE d @@@ 'beta'::ftsquery; }
step "r_commit" { COMMIT; }

# Fresh READ COMMITTED reader (new snapshot per statement, auto-commit).
session "f"
setup           { SET enable_seqscan = off; }
step "f_beta"   { SELECT count(*) AS beta  FROM tdoc WHERE d @@@ 'beta'::ftsquery; }
step "f_alpha"  { SELECT count(*) AS alpha FROM tdoc WHERE d @@@ 'alpha'::ftsquery; }
step "f_gamma"  { SELECT count(*) AS gamma FROM tdoc WHERE d @@@ 'gamma'::ftsquery; }

# Writer.
session "w"
step "w_beta"   { INSERT INTO tdoc SELECT g, to_ftsdoc('beta doc ' || g)
                      FROM generate_series(1000, 1049) g; }
step "w_del"    { DELETE FROM tdoc WHERE id <= 100; }
# VACUUM flushes the pending list and merges segments (recomputing stats).
step "w_vacuum" { VACUUM tdoc; }
step "w_merge"  { SELECT fts_merge('tdoc_bm25'); }
step "w_reuse"  { INSERT INTO tdoc SELECT g, to_ftsdoc('gamma doc ' || g)
                      FROM generate_series(1, 40) g; }

# 1. Snapshot stability: r snapshots (alpha=200, beta=0); w commits 50 beta;
#    r's later reads in the same snapshot are unchanged.
permutation "r_begin" "r_alpha" "w_beta" "r_alpha" "r_beta" "r_commit"

# 2. Pending list is live: w commits beta; a fresh RC read sees all 50.
permutation "w_beta" "f_beta"

# 3. Concurrent VACUUM/merge is MVCC-invisible to an open snapshot.
permutation "r_begin" "r_alpha" "w_vacuum" "r_alpha" "r_commit"
permutation "r_begin" "r_alpha" "w_merge" "r_alpha" "r_commit"

# 4. Delete + VACUUM + heap-slot reuse: fresh read sees 100 alpha (200-100)
#    and 40 gamma, with no resurrection of the deleted alpha via reused slots.
permutation "w_del" "w_vacuum" "w_reuse" "f_alpha" "f_gamma"
