# Test version reconstruction on index-driven RECNO fetch paths.
#
# RECNO updates in place keeping the same TID (am_inplace_update_keeps_tid).
# A committed UPDATE of an indexed column therefore leaves BOTH the old
# (oldkey -> tid) and new (newkey -> tid) secondary index entries pointing
# at the one live tuple.  A REPEATABLE READ reader whose snapshot predates
# the update must still observe the pre-update value through every fetch
# path: a plain index scan, an index-only scan, AND a bitmap heap scan.
#
# The sequential-scan and fetch-by-TID paths carried version reconstruction
# already; this test guards the three index-driven paths
# (recno_index_fetch_tuple for plain/IOS, recno_scan_bitmap_next_tuple for
# bitmap), each of which must call RecnoReconstructVisibleVersion to walk the
# tuple's UNDO-fork version chain and serve the reconstructed prior version to
# an older snapshot.
#
# A fresh reader (s3) starting after the commit must see the new value and
# must NOT find a row at the stale old key.

setup
{
  CREATE TABLE recno_sidx (id int, v int) USING recno;
  INSERT INTO recno_sidx SELECT g, g FROM generate_series(1, 2000) g;
  CREATE INDEX recno_sidx_v ON recno_sidx(v);
  ANALYZE recno_sidx;
}

teardown
{
  DROP TABLE recno_sidx;
}

# s1: REPEATABLE READ reader; snapshot precedes s2's update.
session s1
setup			{ BEGIN ISOLATION LEVEL REPEATABLE READ; }
step s1_snap		{ SELECT count(*) FROM recno_sidx; }
step s1_idxscan
{
  SET enable_seqscan = off; SET enable_bitmapscan = off;
  SET enable_indexscan = on; SET enable_indexonlyscan = off;
  SELECT id, v FROM recno_sidx WHERE v = 100;
}
step s1_ios
{
  SET enable_seqscan = off; SET enable_bitmapscan = off;
  SET enable_indexscan = off; SET enable_indexonlyscan = on;
  SELECT v FROM recno_sidx WHERE v = 100;
}
step s1_bitmap
{
  SET enable_seqscan = off; SET enable_indexscan = off;
  SET enable_indexonlyscan = off; SET enable_bitmapscan = on;
  SELECT id, v FROM recno_sidx WHERE v = 100;
}
step s1_commit		{ COMMIT; }

# s2: writer that moves the indexed value 100 -> 100000 and commits.
session s2
setup			{ BEGIN; }
step s2_update		{ UPDATE recno_sidx SET v = 100000 WHERE id = 100; }
step s2_commit		{ COMMIT; }

# s3: fresh reader after commit; sees the new key, not the stale old key.
session s3
step s3_begin		{ BEGIN ISOLATION LEVEL REPEATABLE READ; }
step s3_oldkey
{
  SET enable_seqscan = off; SET enable_indexscan = off;
  SET enable_indexonlyscan = off; SET enable_bitmapscan = on;
  SELECT id, v FROM recno_sidx WHERE v = 100;
}
step s3_newkey
{
  SET enable_seqscan = off; SET enable_indexscan = off;
  SET enable_indexonlyscan = off; SET enable_bitmapscan = on;
  SELECT id, v FROM recno_sidx WHERE v = 100000;
}
step s3_commit		{ COMMIT; }

# s4: exercises bug #5 -- an in-place UPDATE of a NON-indexed column must
# leave the existing (key -> tid) secondary entry LIVE.  The earlier bug: a
# single index scan that first fetches a genuinely-dead TID set a sticky
# scan-level all_dead, so the very next LIVE TID also reported all_dead=true;
# indexam then set kill_prior_tuple and _bt_killitems marked the live entry
# LP_DEAD -> the live row silently lost its index entry.  Here we DELETE one
# indexed key (making a TID dead) and UPDATE only the non-indexed column of a
# neighbouring row, then range-scan the secondary index across both.  The
# updated row's key is unchanged, so it MUST still be returned.
session s4
step s4_prep
{
  DELETE FROM recno_sidx WHERE v = 500;
  UPDATE recno_sidx SET id = id + 1000000 WHERE v = 501;
}
step s4_scan
{
  SET enable_seqscan = off; SET enable_bitmapscan = off;
  SET enable_indexonlyscan = off; SET enable_indexscan = on;
  SELECT id, v FROM recno_sidx WHERE v BETWEEN 499 AND 502 ORDER BY v;
}
step s4_rescan		{ SELECT id, v FROM recno_sidx WHERE v = 501; }

# Plain index scan: older snapshot must still see v = 100.
permutation s1_snap s2_update s2_commit s1_idxscan s1_commit

# Index-only scan: older snapshot must still see v = 100.
permutation s1_snap s2_update s2_commit s1_ios s1_commit

# Bitmap heap scan: older snapshot must still see v = 100.
permutation s1_snap s2_update s2_commit s1_bitmap s1_commit

# Fresh reader after commit: old key empty, new key finds id = 100;
# concurrently s1's older snapshot still sees v = 100 via bitmap.
permutation s1_snap s2_update s2_commit s3_begin s3_oldkey s3_newkey s1_bitmap s3_commit s1_commit

# bug #5 regression: in-place UPDATE of a non-indexed column (id) after a
# sibling key was DELETEd.  The index-range scan reaches the dead TID then the
# live updated-row TID; the live (v=501 -> tid) entry must survive, so both
# s4_scan (first scan, may kill dead entries) and s4_rescan (second scan, after
# any kill would have taken effect) still find the v=501 row.
permutation s4_prep s4_scan s4_rescan
