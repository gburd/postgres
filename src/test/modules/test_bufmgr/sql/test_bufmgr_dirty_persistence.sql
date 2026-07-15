-- Test - dirty-page persistence under eviction pressure.
--
-- Test that when a modified (dirty) page is evicted under pressure, the buffer
-- manager must have written its contents out, so a subsequent reload returns
-- the modified contents, not the pre-modification ones.
CREATE EXTENSION IF NOT EXISTS pg_buffercache;
CREATE EXTENSION IF NOT EXISTS pg_prewarm;
DROP TABLE IF EXISTS dp_dirt, dp_flood;
CREATE TABLE dp_dirt (id int, payload text) WITH (autovacuum_enabled = off);
INSERT INTO dp_dirt SELECT g, 'orig-' || md5(g::text) FROM generate_series(1, 20000) g;
-- Modify every row: payload becomes a new deterministic value.  All heap pages
-- holding live tuples are now dirty and must be flushed on eviction.
UPDATE dp_dirt SET payload = 'mod-' || md5(id::text);
-- Make the dirty relation fully resident (prewarm hits keep the dirty buffers
-- dirty; it never cleans them), then record the resident peak.
SELECT pg_prewarm('dp_dirt') > 0 AS dirt_prewarmed;

-- Self-calibrating flood until at least one dirty dp_dirt block has
-- been evicted (its resident count drops from the peak).  Since every dp_dirt
-- page is dirty, evicting any of them exercises the write-on-evict path.
DO $$
DECLARE
  peak         bigint;
  now_resident bigint;
  first_blk    bigint;
  last_blk     bigint;
  rounds       int := 0;
BEGIN
  SELECT count(*) INTO peak FROM pg_buffercache
   WHERE relfilenode = pg_relation_filenode('dp_dirt'::regclass)
     AND relforknumber = 0;

  CREATE TABLE dp_flood (id int, payload text) WITH (autovacuum_enabled = off);
  LOOP
    rounds := rounds + 1;
    first_blk := pg_relation_size('dp_flood') / current_setting('block_size')::bigint;
    INSERT INTO dp_flood SELECT g, md5(g::text) FROM generate_series(1, 10000) g;
    last_blk := pg_relation_size('dp_flood') / current_setting('block_size')::bigint - 1;
    PERFORM pg_prewarm('dp_flood', 'buffer', 'main', first_blk, last_blk);
    SELECT count(*) INTO now_resident FROM pg_buffercache
     WHERE relfilenode = pg_relation_filenode('dp_dirt'::regclass)
       AND relforknumber = 0;
    EXIT WHEN now_resident < peak;
    IF rounds > 200 THEN
      RAISE EXCEPTION 'no dirty dp_dirt block evicted after % flood rounds (peak=%, now=%)',
        rounds, peak, now_resident;
    END IF;
  END LOOP;
END $$;
-- Reload correctness: every row must carry its MODIFIED payload.  A row still
-- showing an 'orig-' payload would mean an evicted dirty page was not written
-- out (or was written stale) before reuse.
SELECT count(*) AS unpersisted_rows FROM dp_dirt
 WHERE payload <> 'mod-' || md5(id::text);

DROP TABLE dp_dirt, dp_flood;
