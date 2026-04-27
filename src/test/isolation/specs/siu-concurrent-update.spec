setup
{
    CREATE TABLE iso_siu_test (id int PRIMARY KEY, a int, b int, c text) WITH (fillfactor = 50);
    CREATE INDEX iso_siu_a_idx ON iso_siu_test (a);
    CREATE INDEX iso_siu_b_idx ON iso_siu_test (b);
    INSERT INTO iso_siu_test VALUES (1, 10, 20, 'initial');
    INSERT INTO iso_siu_test VALUES (2, 100, 200, 'other');
}

teardown
{
    DROP TABLE iso_siu_test;
}

session s1
setup           { BEGIN; }
step s1_update_a { UPDATE iso_siu_test SET a = 11 WHERE id = 1; }
step s1_commit   { COMMIT; }

session s2
setup           { BEGIN; }
step s2_update_b { UPDATE iso_siu_test SET b = 21 WHERE id = 1; }
step s2_select_a { SET enable_seqscan = off; SELECT * FROM iso_siu_test WHERE a = 11; }
step s2_select_b { SET enable_seqscan = off; SELECT * FROM iso_siu_test WHERE b = 20; }
step s2_commit   { COMMIT; }

# s1 updates column a, then s2 tries to update column b of same row
# s2 should block until s1 commits, then both updates visible
permutation s1_update_a s2_update_b s1_commit s2_select_a s2_select_b s2_commit

# Both try to update, s2 blocks, verify index scans return correct results after
permutation s1_update_a s2_update_b s1_commit s2_commit
