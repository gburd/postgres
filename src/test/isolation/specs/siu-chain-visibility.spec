setup
{
    CREATE TABLE iso_siu_chain (id int PRIMARY KEY, a int, b int, val text) WITH (fillfactor = 50);
    CREATE INDEX iso_siu_chain_a_idx ON iso_siu_chain (a);
    CREATE INDEX iso_siu_chain_b_idx ON iso_siu_chain (b);
    INSERT INTO iso_siu_chain VALUES (1, 10, 20, 'v0');
}

teardown
{
    DROP TABLE iso_siu_chain;
}

session s1
setup           { BEGIN; }
step s1_upd_a    { UPDATE iso_siu_chain SET a = 11, val = 'v1' WHERE id = 1; }
step s1_upd_b    { UPDATE iso_siu_chain SET b = 21, val = 'v2' WHERE id = 1; }
step s1_upd_a2   { UPDATE iso_siu_chain SET a = 12, val = 'v3' WHERE id = 1; }
step s1_commit   { COMMIT; }

session s2
setup           { BEGIN ISOLATION LEVEL REPEATABLE READ; }
step s2_snap     { SELECT 1; }
step s2_read_a   { SET enable_seqscan = off; SELECT * FROM iso_siu_chain WHERE a = 10; }
step s2_read_b   { SET enable_seqscan = off; SELECT * FROM iso_siu_chain WHERE b = 20; }
step s2_commit   { COMMIT; }

session s3
setup           { BEGIN ISOLATION LEVEL REPEATABLE READ; }
step s3_snap     { SELECT 1; }
step s3_read_a   { SET enable_seqscan = off; SELECT * FROM iso_siu_chain WHERE a = 12; }
step s3_read_b   { SET enable_seqscan = off; SELECT * FROM iso_siu_chain WHERE b = 21; }
step s3_commit   { COMMIT; }

# s2 takes snapshot before s1's updates, should see original values
# s3 takes snapshot after s1 commits, should see final values
permutation s2_snap s1_upd_a s1_upd_b s1_upd_a2 s1_commit s3_snap s2_read_a s2_read_b s3_read_a s3_read_b s2_commit s3_commit
