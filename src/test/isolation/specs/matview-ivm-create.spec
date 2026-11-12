# CREATE MATERIALIZED VIEW ... WITH (incremental_view_maintenance) blocks
# writers of its base table, and a writer that was already running when
# CREATE started must still be reflected in the view: nothing captures
# changes made before the view existed, so the view has to be populated
# after that writer commits.

setup
{
  CREATE TABLE ivm_base (k int, v int);
  INSERT INTO ivm_base VALUES (1, 10);
}

teardown
{
  DROP TABLE ivm_base CASCADE;
}

session writer
step w_begin   { BEGIN; }
step w_insert  { INSERT INTO ivm_base VALUES (1, 20), (2, 30); }
step w_commit  { COMMIT; }
step w_abort   { ROLLBACK; }
step w_insert2 { INSERT INTO ivm_base VALUES (3, 40); }

session creator
step c_create  {
  CREATE MATERIALIZED VIEW ivm_mv WITH (incremental_view_maintenance) AS
    SELECT k, count(*), sum(v) FROM ivm_base GROUP BY k;
}
step c_rr      { BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT 1; }
step c_create_rr  {
  CREATE MATERIALIZED VIEW ivm_mv WITH (incremental_view_maintenance) AS
    SELECT k, count(*), sum(v) FROM ivm_base GROUP BY k;
}
step c_begin   { BEGIN; }
step c_commit  { COMMIT; }
step c_show    { SELECT * FROM ivm_mv ORDER BY k; }

# The writer is running when CREATE starts; CREATE waits, and its view
# includes the writer's rows.
permutation w_begin w_insert c_create w_commit c_show

# Same, with the writer rolling back.
permutation w_begin w_insert c_create w_abort c_show

# A REPEATABLE READ transaction whose snapshot predates the writer's commit
# still populates the view from after it.
permutation c_rr w_begin w_insert c_create_rr w_commit c_commit c_show

# A writer that starts after CREATE (in an open transaction) waits until
# CREATE commits, and its change is then maintained into the view.
permutation c_begin c_create w_insert2 c_commit c_show
