# Two transactions that each change two base tables, in opposite orders,
# must not deadlock on the view locks taken at commit.

setup
{
  CREATE TABLE ivm_la (k int, v int);
  CREATE TABLE ivm_lb (k int, v int);
  CREATE MATERIALIZED VIEW ivm_lva WITH (incremental_view_maintenance) AS
    SELECT k, count(*) AS n FROM ivm_la GROUP BY k;
  CREATE MATERIALIZED VIEW ivm_lvb WITH (incremental_view_maintenance) AS
    SELECT k, count(*) AS n FROM ivm_lb GROUP BY k;
}

teardown
{
  DROP TABLE ivm_la, ivm_lb CASCADE;
}

session s1
step s1_begin  { BEGIN; }
step s1_a      { INSERT INTO ivm_la VALUES (1, 1); }
step s1_b      { INSERT INTO ivm_lb VALUES (1, 1); }
step s1_commit { COMMIT; }

session s2
step s2_begin  { BEGIN; }
step s2_b      { INSERT INTO ivm_lb VALUES (2, 2); }
step s2_a      { INSERT INTO ivm_la VALUES (2, 2); }
step s2_commit { COMMIT; }

session s3
step s3_show   { SELECT 'a', * FROM ivm_lva UNION ALL SELECT 'b', * FROM ivm_lvb ORDER BY 1, 2; }

permutation s1_begin s2_begin s1_a s2_b s1_b s2_a s1_commit s2_commit s3_show
permutation s1_begin s2_begin s1_a s2_b s1_b s2_a s2_commit s1_commit s3_show
