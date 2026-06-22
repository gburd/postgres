# Test SERIALIZABLE conflict detection for RECNO table access method.
#
# This tests write-skew and read-write dependency detection via
# PostgreSQL's predicate locking (SSI) infrastructure.  RECNO integrates
# with predicate.c by acquiring SIREAD locks in the scan path and calling
# CheckForSerializableConflictIn() in the DML paths.
#
# The classic write-skew scenario (doctors on call) is correctly detected:
# the second transaction to commit receives serialization_failure.

setup
{
  CREATE TABLE recno_doctors (id int, name text, on_call boolean) USING recno;
  INSERT INTO recno_doctors VALUES (1, 'Alice', true);
  INSERT INTO recno_doctors VALUES (2, 'Bob', true);
}

teardown
{
  DROP TABLE recno_doctors;
}

session s1
setup			{ BEGIN ISOLATION LEVEL SERIALIZABLE; }
step s1_read_count	{ SELECT count(*) FROM recno_doctors WHERE on_call = true; }
step s1_update		{ UPDATE recno_doctors SET on_call = false WHERE id = 1; }
step s1_commit		{ COMMIT; }

session s2
setup			{ BEGIN ISOLATION LEVEL SERIALIZABLE; }
step s2_read_count	{ SELECT count(*) FROM recno_doctors WHERE on_call = true; }
step s2_update		{ UPDATE recno_doctors SET on_call = false WHERE id = 2; }
step s2_commit		{ COMMIT; }

session s3
step s3_read		{ SELECT id, name, on_call FROM recno_doctors ORDER BY id; }

# Classic write skew: Both read count=2, both set their own row to false.
# SSI detects the rw-antidependency cycle: s2's commit fails with
# serialization_failure because s1 committed first.
permutation s1_read_count s2_read_count s1_update s2_update s1_commit s2_commit s3_read
