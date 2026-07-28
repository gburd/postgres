-- Test - a page read once and never again does not indefinitely retain its
-- buffer under sustained allocation pressure.
--
-- This is the universally-true direction of scan resistance: one-touch pages
-- are reclaimable, so pressure cannot be indefinitely denied.
-- Every correct buffer manager satisfies this, regardless of replacement policy, and
-- the test asserts only an end-state (the one-touch relation becomes fully
-- non-resident under enough pressure), never which buffer is chosen.
CREATE EXTENSION IF NOT EXISTS pg_buffercache;
CREATE EXTENSION IF NOT EXISTS pg_prewarm;
DROP TABLE IF EXISTS sr_oneshot, sr_flood;
CREATE TABLE sr_oneshot (id int, payload text) WITH (autovacuum_enabled = off);
INSERT INTO sr_oneshot SELECT g, md5(g::text) FROM generate_series(1, 2000) g;
-- Touch every sr_oneshot block exactly once (prewarm), then never again.
SELECT pg_prewarm('sr_oneshot') > 0 AS oneshot_resident;

-- Self-calibrating flood until the one-touch relation is fully
-- reclaimed (no resident blocks left), bounded by a generous cap.
DO $$
DECLARE
  now_resident bigint;
  first_blk    bigint;
  last_blk     bigint;
  rounds       int := 0;
BEGIN
  CREATE TABLE sr_flood (id int, payload text) WITH (autovacuum_enabled = off);
  LOOP
    rounds := rounds + 1;
    first_blk := pg_relation_size('sr_flood') / current_setting('block_size')::bigint;
    INSERT INTO sr_flood SELECT g, md5(g::text) FROM generate_series(1, 10000) g;
    last_blk := pg_relation_size('sr_flood') / current_setting('block_size')::bigint - 1;
    PERFORM pg_prewarm('sr_flood', 'buffer', 'main', first_blk, last_blk);
    SELECT count(*) INTO now_resident FROM pg_buffercache
     WHERE relfilenode = pg_relation_filenode('sr_oneshot'::regclass)
       AND relforknumber = 0;
    EXIT WHEN now_resident = 0;
    IF rounds > 200 THEN
      RAISE EXCEPTION 'sr_oneshot still has % resident blocks after % flood rounds',
        now_resident, rounds;
    END IF;
  END LOOP;
END $$;
-- The one-touch relation retained no buffers under pressure.
SELECT count(*) AS still_resident FROM pg_buffercache
 WHERE relfilenode = pg_relation_filenode('sr_oneshot'::regclass)
   AND relforknumber = 0;

DROP TABLE sr_oneshot, sr_flood;
