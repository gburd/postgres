# INSERT ... ON CONFLICT DO NOTHING / DO UPDATE on a BARK unique index.
#
# Two sessions speculatively insert the same key with ON CONFLICT.  The BARK
# aminsert path must detect the other session's in-progress speculative
# insertion and wait on its speculative token (not its whole xact), then
# re-check: exactly one row ends up inserted, and the loser does nothing (DO
# NOTHING) or updates the winner's row (DO UPDATE).  This mirrors nbtree's
# insert-conflict-do-nothing / do-update specs, with the uniqueness enforced by
# a BARK index instead of a btree primary key.
#
# The convention here is that session 1 always ends up inserting (the winner)
# and session 2 always ends up doing nothing / updating (the loser).

setup
{
  CREATE TABLE ints (key int, val text);
  CREATE UNIQUE INDEX ints_bark_key ON ints USING bark (key);
}

teardown
{
  DROP TABLE ints;
}

session s1
setup
{
  BEGIN ISOLATION LEVEL READ COMMITTED;
}
step donothing1 { INSERT INTO ints(key, val) VALUES(1, 'donothing1') ON CONFLICT (key) DO NOTHING; }
step doupdate1  { INSERT INTO ints(key, val) VALUES(1, 'doupdate1') ON CONFLICT (key) DO UPDATE SET val = excluded.val; }
step c1 { COMMIT; }
step a1 { ABORT; }

session s2
setup
{
  BEGIN ISOLATION LEVEL READ COMMITTED;
}
step donothing2 { INSERT INTO ints(key, val) VALUES(1, 'donothing2') ON CONFLICT (key) DO NOTHING; }
step doupdate2  { INSERT INTO ints(key, val) VALUES(1, 'doupdate2') ON CONFLICT (key) DO UPDATE SET val = excluded.val; }
step select2 { SELECT key, val FROM ints ORDER BY key; }
step c2 { COMMIT; }

# s2 block-waits on s1's speculative insertion, then does nothing when s1
# commits (one row), or inserts its own when s1 aborts.
permutation donothing1 donothing2 c1 select2 c2
permutation donothing1 donothing2 a1 select2 c2

# Same race with DO UPDATE: s2 waits on s1's token, then updates s1's row
# (one row, s2's value) when s1 commits, or inserts its own when s1 aborts.
permutation doupdate1 doupdate2 c1 select2 c2
permutation doupdate1 doupdate2 a1 select2 c2
