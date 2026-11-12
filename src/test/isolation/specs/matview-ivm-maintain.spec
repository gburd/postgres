# Concurrent writers to the base table of an incrementally maintained
# materialized view.  Each committer recomputes the groups it touched
# under the view's ExclusiveLock with a snapshot taken after the lock, so
# the view must equal its query after every commit whatever the
# interleaving and isolation level.

setup
{
  CREATE TABLE ivm_c (k int, v int);
  INSERT INTO ivm_c VALUES (1, 1), (2, 2);
  CREATE MATERIALIZED VIEW ivm_cm WITH (incremental_view_maintenance) AS
    SELECT k, count(*) AS n, sum(v) AS s FROM ivm_c GROUP BY k;
  CREATE FUNCTION ivm_cm_ok() RETURNS bool LANGUAGE sql AS $$
    SELECT NOT EXISTS (
      (SELECT * FROM ivm_cm EXCEPT ALL
       SELECT k, count(*), sum(v) FROM ivm_c GROUP BY k)
      UNION ALL
      (SELECT k, count(*), sum(v) FROM ivm_c GROUP BY k EXCEPT ALL
       SELECT * FROM ivm_cm))
  $$;
}

teardown
{
  DROP TABLE ivm_c CASCADE;
  DROP FUNCTION ivm_cm_ok();
}

session s1
step s1_begin    { BEGIN; }
step s1_rr       { BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT 1; }
step s1_ins      { INSERT INTO ivm_c VALUES (1, 10); }
step s1_commit   { COMMIT; }
step s1_abort    { ROLLBACK; }
step s1_prepare  { PREPARE TRANSACTION 'ivm_p'; }
step s1_cprep    { COMMIT PREPARED 'ivm_p'; }

session s2
step s2_begin    { BEGIN; }
step s2_rr       { BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT 1; }
step s2_ins      { INSERT INTO ivm_c VALUES (1, 100); }
step s2_upd      { UPDATE ivm_c SET v = v + 1000 WHERE k = 2; }
step s2_commit   { COMMIT; }

session s3
step s3_check    { SELECT ivm_cm_ok(); }
step s3_show     { SELECT * FROM ivm_cm ORDER BY k; }

# two writers to the same group; the second waits for the first's commit
permutation s1_begin s2_begin s1_ins s2_ins s1_commit s2_commit s3_check s3_show

# the same, committed in the other order
permutation s1_begin s2_begin s1_ins s2_ins s2_commit s1_commit s3_check s3_show

# writers to different groups
permutation s1_begin s2_begin s1_ins s2_upd s1_commit s2_commit s3_check s3_show

# REPEATABLE READ writers whose snapshots predate each other's commit
permutation s1_rr s2_rr s1_ins s2_ins s1_commit s2_commit s3_check s3_show

# a writer that aborts leaves the view as it was
permutation s1_begin s2_begin s1_ins s2_ins s1_abort s2_commit s3_check s3_show

# a reader during maintenance is not blocked and sees the committed view
permutation s1_begin s1_ins s3_check s1_commit s3_check

# a prepared transaction maintains the view at PREPARE, and its changes and
# the view's become visible together at COMMIT PREPARED.  (It holds the
# view lock until then, so a second writer's COMMIT would wait; the
# isolation tester cannot see a wait on a prepared transaction, so that is
# not tested here.)
permutation s1_begin s1_ins s1_prepare s3_check s3_show s1_cprep s3_check s3_show
