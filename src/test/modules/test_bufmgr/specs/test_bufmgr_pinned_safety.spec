# Test - pinned-buffer safety under eviction pressure.
#
# Property (must hold for ANY correct buffer manager): a pinned buffer is never
# selected as an eviction victim. While one backend holds a pin on a page,
# another backend can drive allocation pressure far exceeding the whole shared
# buffer pool, and the pinned page must remain resident and its contents valid.
#
# The pin is held across statements using an existing PostgreSQL facility -- a
# suspended cursor. A plain transaction does not keep heap-page pins between
# statements (each scan drops its pins when it finishes), but a cursor that has
# fetched into its scan keeps the current page pinned until it advances or
# closes.
#
# A control "probe" block, resident but UNPINNED, guards against a vacuous
# pass: the flood must actually exceed the pool, which we prove by asserting the
# probe block is evicted while the pinned block survives. If the flood were too
# small, the probe would remain resident and the test would fail loudly instead
# of passing for the wrong reason.

setup
{
  CREATE EXTENSION IF NOT EXISTS pg_buffercache;
  CREATE EXTENSION IF NOT EXISTS pg_prewarm;
  CREATE TABLE pinned (id int, payload text) WITH (autovacuum_enabled = off);
  INSERT INTO pinned SELECT g, md5(g::text) FROM generate_series(1, 500) g;
  CREATE TABLE probe (id int, payload text) WITH (autovacuum_enabled = off);
  INSERT INTO probe SELECT g, md5(g::text) FROM generate_series(1, 500) g;
  CREATE TABLE flood (id int, payload text) WITH (autovacuum_enabled = off);
}
setup
{
  DO $$
  DECLARE
    target bigint;
    rnum     int := 0;
  BEGIN
    SELECT setting::bigint * current_setting('block_size')::bigint * 2 INTO target
      FROM pg_settings WHERE name = 'shared_buffers';
    WHILE pg_relation_size('flood'::regclass) < target LOOP
      INSERT INTO flood SELECT g, md5(g::text) FROM generate_series(rnum + 1, rnum + 10000) g;
      rnum := rnum + 10000;
    END LOOP;
  END $$;
}
setup
{
  -- Make the unpinned control probe resident right before the test runs.
  -- (PERFORM, not SELECT: the raw prewarmed-block count varies with BLCKSZ.)
  DO $$ BEGIN PERFORM pg_prewarm('probe'); END $$;
}

teardown
{
  DROP TABLE IF EXISTS pinned;
  DROP TABLE IF EXISTS probe;
  DROP TABLE IF EXISTS flood;
}

# Holds a pin on pinned's block 0 via a suspended cursor.
session pinholder
step p_pin
{
  BEGIN;
  DECLARE cur CURSOR FOR SELECT id, payload FROM pinned;
  FETCH NEXT FROM cur;
}
# The pinned block must still be resident AND still show a live pin held by us,
# while the unpinned control probe block must have been evicted by the flood.
step p_check
{
  SELECT
    (SELECT count(*) > 0 FROM pg_buffercache
       WHERE relfilenode = pg_relation_filenode('pinned'::regclass)
         AND relforknumber = 0 AND relblocknumber = 0) AS pinned_resident,
    (SELECT bool_or(pinning_backends >= 1) FROM pg_buffercache
       WHERE relfilenode = pg_relation_filenode('pinned'::regclass)
         AND relforknumber = 0 AND relblocknumber = 0) AS pinned_pinned,
    (SELECT count(*) = 0 FROM pg_buffercache
       WHERE relfilenode = pg_relation_filenode('probe'::regclass)
         AND relforknumber = 0 AND relblocknumber = 0) AS probe_evicted;
}
# The contents of the pinned page are intact and unchanged (id=1 lives on
# block 0): read it back and confirm its payload still matches.
step p_verify
{
  SELECT (payload = md5(id::text)) AS content_ok FROM pinned WHERE id = 1;
}
step p_commit { COMMIT; }

# Drives allocation pressure far larger than the whole pool, ring-free.
session flooder
step f_flood
{
  SELECT pg_prewarm('flood') > 0 AS flooded;
}

# Pin, flood past the pool, then confirm the pinned page survived intact while
# the unpinned probe did not.
permutation p_pin f_flood p_check p_verify p_commit
