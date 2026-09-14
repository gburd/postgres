# heap control for flux_update_victim.spec: same steps, same expected output.
# FLUX: an UPDATE that waited for a row and then became a deadlock victim
# must not leave its increments behind.  Every value must equal the number
# of committed increments recorded in fdv_hist (same transaction), as in
# heap.

setup
{
  CREATE TABLE fdv (id int PRIMARY KEY, v int) USING heap;
  INSERT INTO fdv VALUES (1, 0), (2, 0);
  CREATE TABLE fdv_hist (id int);
}

teardown
{
  DROP TABLE fdv, fdv_hist;
}

session s1
setup		{ SET deadlock_timeout = '10s'; BEGIN; }
step s1u1	{ UPDATE fdv SET v = v + 1 WHERE id = 1; INSERT INTO fdv_hist VALUES (1); }
step s1u2	{ UPDATE fdv SET v = v + 1 WHERE id = 2; INSERT INTO fdv_hist VALUES (2); }
step s1c	{ COMMIT; }

session s2
setup		{ SET deadlock_timeout = '10ms'; BEGIN; }
step s2u1	{ UPDATE fdv SET v = v + 1 WHERE id = 1; INSERT INTO fdv_hist VALUES (1); }
step s2u2	{ UPDATE fdv SET v = v + 1 WHERE id = 2; INSERT INTO fdv_hist VALUES (2); }
step s2c	{ COMMIT; }

session s3
setup		{ SET deadlock_timeout = '10s'; BEGIN; }
step s3u1	{ UPDATE fdv SET v = v + 1 WHERE id = 1; INSERT INTO fdv_hist VALUES (1); }
step s3u2	{ UPDATE fdv SET v = v + 1 WHERE id = 2; INSERT INTO fdv_hist VALUES (2); }
step s3c	{ COMMIT; }

session s4
step chk	{ SELECT f.id, f.v, count(h.id) AS committed FROM fdv f LEFT JOIN fdv_hist h USING (id) GROUP BY f.id, f.v ORDER BY f.id; }

# s2 waits for s1 on row 1, after updating row 2; then s1 waits on row 2
# -> s2 is the victim (short deadlock_timeout) while its row-1 wait is
# granted the row only after s1's commit.
permutation s2u2 s1u1 s2u1 s1u2 s1c s2c chk
# Same, with s3 queued on row 2 behind s2.
permutation s2u2 s1u1 s3u2 s2u1 s1u2 s1c s2c s3c chk
# s2's first update waited on s1 and succeeded after s1 committed; its
# second update then deadlocks with s3.
permutation s1u1 s3u2 s2u1 s1c s2u2 s3u1 s2c s3c chk
