# CREATE INDEX CONCURRENTLY / REINDEX CONCURRENTLY correctness for bm25.
#
# The concurrent build runs in two phases and must capture rows written by
# other backends while it runs.  pg_fts routes every aminsert to the pending
# write buffer (immediately searchable), so a doc inserted during the build's
# validation phase must appear in the finished index.  This spec interleaves a
# CIC / RIC in one session with committed INSERTs in another and then verifies
# the index returns every live row.

setup
{
    CREATE EXTENSION IF NOT EXISTS pg_fts;
    CREATE TABLE cic (id int primary key, d ftsdoc);
    INSERT INTO cic SELECT g, to_ftsdoc('alpha doc ' || g)
        FROM generate_series(1, 100) g;
}

teardown
{
    DROP TABLE cic;
}

session "builder"
step "b_cic"   { CREATE INDEX CONCURRENTLY cic_bm25 ON cic USING bm25 (d); }
step "b_ric"   { REINDEX INDEX CONCURRENTLY cic_bm25; }
step "b_check" { SET enable_seqscan = off;
                 SELECT count(*) AS alpha FROM cic WHERE d @@@ 'alpha'::ftsquery;
                 SELECT count(*) AS beta  FROM cic WHERE d @@@ 'beta'::ftsquery; }

session "writer"
step "w_beta"  { INSERT INTO cic SELECT g, to_ftsdoc('beta doc ' || g)
                     FROM generate_series(200, 249) g; }

# The isolation tester serializes steps, so this does not reproduce a true
# mid-build race, but it does verify that CIC and RIC complete successfully and
# that a concurrently-committed INSERT is fully reflected: after CIC (100 alpha,
# 0 beta), a writer commits 50 beta, then REINDEX CONCURRENTLY rebuilds; the
# final index must return 100 alpha and 50 beta.
permutation "b_cic" "w_beta" "b_ric" "b_check"
