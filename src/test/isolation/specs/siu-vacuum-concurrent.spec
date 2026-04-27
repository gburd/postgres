setup
{
    CREATE TABLE iso_siu_vacuum (id int PRIMARY KEY, a int, b int, filler text) WITH (fillfactor = 50);
    CREATE INDEX iso_siu_vacuum_a_idx ON iso_siu_vacuum (a);
    CREATE INDEX iso_siu_vacuum_b_idx ON iso_siu_vacuum (b);
    INSERT INTO iso_siu_vacuum VALUES (1, 10, 20, 'start');
    INSERT INTO iso_siu_vacuum VALUES (2, 100, 200, 'other');
}

teardown
{
    DROP TABLE iso_siu_vacuum;
}

session s1
setup           { BEGIN; }
step s1_update1  { UPDATE iso_siu_vacuum SET a = 11 WHERE id = 1; }
step s1_update2  { UPDATE iso_siu_vacuum SET b = 21 WHERE id = 1; }
step s1_commit   { COMMIT; }
step s1_check_a  { SET enable_seqscan = off; SELECT * FROM iso_siu_vacuum WHERE a = 11; }
step s1_check_b  { SET enable_seqscan = off; SELECT * FROM iso_siu_vacuum WHERE b = 21; }

session s2
step s2_vacuum   { VACUUM iso_siu_vacuum; }

# Create SIU chain, commit, vacuum, then verify index scans.
# VACUUM preserves dead SIU intermediate tuples (those with
# HEAP_INDEXED_UPDATED) because fresh index entries point directly
# at them.  Pruning them would cause those entries to dangle.
# Both index checks return the correct row.
permutation s1_update1 s1_update2 s1_commit s2_vacuum s1_check_a s1_check_b
