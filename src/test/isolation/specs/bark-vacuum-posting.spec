# VACUUM of a BARK POSTING entry, concurrent with a scan positioned inside it.
#
# 60000 rows of one key, every 10th deleted.  The survivors' sbm encodes
# larger than the whole set did (removing members breaks up all-ones
# vectors), which the room a POSTING entry reserves for its set absorbs, so
# VACUUM rewrites each entry in place, in one WAL record, with no page split
# and no re-insert of survivors elsewhere in the index.  A cursor that has
# fetched one row before the VACUUM must then fetch every live row exactly
# once: none twice (as a re-insert into another entry would cause) and none
# missed.
#
# The cursor uses a plain index scan.  Its fetched rows are collected through
# a PL/pgSQL loop so the output is a count, not 54000 rows.  Afterwards an
# index-only scan, which trusts the heap pages VACUUM has marked all-visible,
# must count only the live rows: a dead TID left in the index would count.
#
# VACUUM takes a cleanup lock on every leaf, so it waits while the cursor
# holds its pin on a leaf; isolationtester does not see that wait, hence (*).
# VACUUM finishes once s1fetchall has run the scan to its end, and is reported
# after s1fetchall either way.

setup
{
  CREATE TABLE bark_vp (a int, b int) WITH (autovacuum_enabled = off);
  CREATE INDEX bark_vp_idx ON bark_vp USING bark (a);
  INSERT INTO bark_vp SELECT 1, g FROM generate_series(1, 60000) g;
  DELETE FROM bark_vp WHERE b % 10 = 0;
  CREATE TABLE bark_vp_got (b int);
  CREATE FUNCTION bark_vp_fetch(cur refcursor, maxrows int) RETURNS void
  LANGUAGE plpgsql AS $$
  DECLARE
    v int;
  BEGIN
    FOR i IN 1 .. maxrows LOOP
      FETCH cur INTO v;
      EXIT WHEN NOT FOUND;
      INSERT INTO bark_vp_got VALUES (v);
    END LOOP;
  END $$;
}

teardown
{
  DROP TABLE bark_vp, bark_vp_got;
  DROP FUNCTION bark_vp_fetch(refcursor, int);
}

session s1
setup
{
  SET enable_seqscan = off;
  SET enable_bitmapscan = off;
  SET enable_indexonlyscan = off;
}
step s1open		{ BEGIN; DECLARE c CURSOR FOR SELECT b FROM bark_vp WHERE a = 1; }
step s1fetch1	{ SELECT bark_vp_fetch('c', 1); }
step s1fetchall	{ SELECT bark_vp_fetch('c', 1000000); }
step s1count
{
  SELECT count(*) AS fetched, count(DISTINCT b) AS distinct_rows,
         count(*) FILTER (WHERE b % 10 = 0) AS deleted_rows
    FROM bark_vp_got;
}
step s1commit	{ COMMIT; }

session s2
step s2vacuum	{ VACUUM bark_vp; }
step s2count
{
  SET enable_seqscan = off;
  SET enable_bitmapscan = off;
  SET enable_indexonlyscan = on;
  SELECT count(*) AS index_only_rows FROM bark_vp WHERE a = 1;
}

permutation s1open s1fetch1 s2vacuum(*) s1fetchall s1count s1commit s2count
