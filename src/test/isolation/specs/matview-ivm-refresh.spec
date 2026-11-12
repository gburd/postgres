# REFRESH of an incrementally maintained materialized view, racing with
# writers whose commit maintains it.
#
# A committing writer holds the view lock from its commit pass until its
# commit is visible.  REFRESH must read the base table with a snapshot
# taken after it gets the view lock, or it overwrites the view with data
# that lacks the writer's change.  To hold a writer inside its commit
# pass, give the base table a second view, created later so it is
# maintained second, and hold that one with an open REFRESH.

setup
{
  CREATE TABLE ivm_r (k int, v int);
  INSERT INTO ivm_r VALUES (1, 1), (2, 2);
  CREATE MATERIALIZED VIEW ivm_r1 WITH (incremental_view_maintenance) AS
    SELECT k, count(*) AS n, sum(v) AS s FROM ivm_r GROUP BY k;
  CREATE UNIQUE INDEX ON ivm_r1 (k);
  CREATE MATERIALIZED VIEW ivm_r2 WITH (incremental_view_maintenance) AS
    SELECT count(*) AS n FROM ivm_r;
  CREATE FUNCTION ivm_r_ok() RETURNS TABLE (r1 bool, r2 bool)
  LANGUAGE sql AS $$
    SELECT NOT EXISTS (
      (SELECT * FROM ivm_r1 EXCEPT ALL
       SELECT k, count(*), sum(v) FROM ivm_r GROUP BY k)
      UNION ALL
      (SELECT k, count(*), sum(v) FROM ivm_r GROUP BY k EXCEPT ALL
       SELECT * FROM ivm_r1)),
     (SELECT n FROM ivm_r2) = (SELECT count(*) FROM ivm_r)
  $$;
}

teardown
{
  DROP TABLE ivm_r CASCADE;
  DROP FUNCTION ivm_r_ok();
}

session w
step w_begin  { BEGIN; }
step w_ins    { INSERT INTO ivm_r VALUES (1, 10), (3, 30); }
step w_commit { COMMIT; }

session r
step r_refresh  { REFRESH MATERIALIZED VIEW ivm_r1; }
step r_refreshc { REFRESH MATERIALIZED VIEW CONCURRENTLY ivm_r1; }
step r_rr       { BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT 1; }
step r_rrrefresh { REFRESH MATERIALIZED VIEW ivm_r1; }
step r_rrnodata { REFRESH MATERIALIZED VIEW ivm_r1 WITH NO DATA; }
step r_end      { COMMIT; }

session h
step h_hold   { BEGIN; REFRESH MATERIALIZED VIEW ivm_r2; }
step h_commit { COMMIT; }
step h_check  { SELECT * FROM ivm_r_ok(); }
step h_show   { SELECT * FROM ivm_r1 ORDER BY k; }

# w's commit pass maintains ivm_r1, then waits for ivm_r2 while holding
# ivm_r1's lock; REFRESH of ivm_r1 starts and waits for w
permutation h_hold w_begin w_ins w_commit r_refresh h_commit h_check h_show
permutation h_hold w_begin w_ins w_commit r_refreshc h_commit h_check h_show

# a REFRESH that holds the view makes the writer's commit wait, and the
# writer then maintains the refreshed view
permutation w_begin w_ins h_hold w_commit h_commit h_check

# REFRESH in a REPEATABLE READ transaction is refused, since its snapshot
# may predate a maintainer that committed while it waited for the lock;
# WITH NO DATA reads nothing and is allowed
permutation r_rr r_rrrefresh r_end h_check
permutation r_rr r_rrnodata r_end
