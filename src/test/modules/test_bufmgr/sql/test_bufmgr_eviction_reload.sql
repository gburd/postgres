-- Test - eviction and reload correctness:
--
-- Property (must hold for ANY correct buffer manager, regardless of its
-- replacement policy): under allocation pressure whose working set exceeds
-- shared_buffers, at least one previously-resident, unpinned page is evicted;
-- reloading any evicted page returns its original contents; and the manager
-- makes progress (no "no unpinned buffers available" error).
--
-- Observed only through pg_buffercache residency (a row present for a relation
-- block == that block is resident) and through the page contents themselves,
-- never through usagecount or any replacement-policy internal. The test never
-- predicts which page is chosen as the victim.
CREATE EXTENSION IF NOT EXISTS pg_buffercache;
CREATE EXTENSION IF NOT EXISTS pg_prewarm;
DROP TABLE IF EXISTS er_victim, er_flood;
-- Uniquely identifiable pages: each row's payload is a deterministic function
-- of its id, so any block read back can be verified without assuming how many
-- rows fall on a page or which page a given row lands on.
CREATE TABLE er_victim (id int, payload text) WITH (autovacuum_enabled = off);
INSERT INTO er_victim SELECT g, md5(g::text) FROM generate_series(1, 20000) g;
SELECT pg_prewarm('er_victim') > 0 AS victim_prewarmed;

-- Self-calibrating flood: grow a throwaway relation and prewarm each newly
-- added block range until at least one er_victim block that was
-- resident has been evicted i.e. er_victim's resident-block count has
-- strictly dropped from its post-prewarm peak. This asserts an observed
-- end-state ("reclaim happened") rather than a hand-tuned flood size, and is
-- bounded by a generous cap so a real failure still terminates the test.
DO $$
DECLARE
  peak         bigint;
  now_resident bigint;
  first_blk    bigint;
  last_blk     bigint;
  rounds       int := 0;
BEGIN
  SELECT count(*) INTO peak FROM pg_buffercache
   WHERE relfilenode = pg_relation_filenode('er_victim'::regclass)
     AND relforknumber = 0;

  CREATE TABLE er_flood (id int, payload text) WITH (autovacuum_enabled = off);
  LOOP
    rounds := rounds + 1;
    first_blk := pg_relation_size('er_flood') / current_setting('block_size')::bigint;
    INSERT INTO er_flood SELECT g, md5(g::text) FROM generate_series(1, 10000) g;
    last_blk := pg_relation_size('er_flood') / current_setting('block_size')::bigint - 1;
    PERFORM pg_prewarm('er_flood', 'buffer', 'main', first_blk, last_blk);
    SELECT count(*) INTO now_resident FROM pg_buffercache
     WHERE relfilenode = pg_relation_filenode('er_victim'::regclass)
       AND relforknumber = 0;
    EXIT WHEN now_resident < peak;    -- a victim block was reclaimed
    IF rounds > 200 THEN
      RAISE EXCEPTION 'no er_victim block evicted after % flood rounds (peak=%, now=%)',
        rounds, peak, now_resident;
    END IF;
  END LOOP;
END $$;
-- Reload correctness: this full scan re-reads every block from storage,
-- including the evicted ones, and every row must still carry its original
-- payload.  A nonzero count would mean a reloaded page returned wrong contents.
SELECT count(*) AS corrupt_rows FROM er_victim WHERE payload <> md5(id::text);

DROP TABLE er_victim, er_flood;
