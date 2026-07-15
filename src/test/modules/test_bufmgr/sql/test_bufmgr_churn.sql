-- Test - repeated eviction and reload cycles.
--
-- Several relations whose combined working set exceeds the pool, cycled many
-- times so most blocks are reloaded from storage on every pass. Contents must
-- remain correct after many replacement/reload cycles.  The test never asserts
-- which buffers are selected as victims.
--
-- Sizes are derived from shared_buffers in bytes (not a fixed row count), so
-- the combined working set exceeds the pool at any BLCKSZ. Cycling is done
-- with ring-free pg_prewarm so it drives real pool-wide reuse.
CREATE EXTENSION IF NOT EXISTS pg_buffercache;
CREATE EXTENSION IF NOT EXISTS pg_prewarm;
DROP TABLE IF EXISTS ch_a, ch_b, ch_c;
CREATE TABLE ch_a (id int, payload text) WITH (autovacuum_enabled = off);
CREATE TABLE ch_b (id int, payload text) WITH (autovacuum_enabled = off);
CREATE TABLE ch_c (id int, payload text) WITH (autovacuum_enabled = off);
-- Fill each relation to roughly one whole pool in bytes, so the three combined
-- are ~3x shared_buffers and no single pass can stay resident.
DO $$
DECLARE
  target   bigint;
  tbl      text;
  rnum       int;
BEGIN
  SELECT setting::bigint * current_setting('block_size')::bigint INTO target
    FROM pg_settings WHERE name = 'shared_buffers';
  FOREACH tbl IN ARRAY ARRAY['ch_a','ch_b','ch_c'] LOOP
    rnum := 0;
    WHILE pg_relation_size(tbl::regclass) < target LOOP
      EXECUTE format(
        'INSERT INTO %I SELECT g, %L || md5(g::text) FROM generate_series(%s, %s) g',
        tbl, right(tbl, 1) || '-', rnum + 1, rnum + 10000);
      rnum := rnum + 10000;
    END LOOP;
  END LOOP;
END $$;
-- Cycle through all three many times; each prewarm reloads a whole pool's worth
-- of blocks, evicting the others.
DO $$
BEGIN
  FOR i IN 1..15 LOOP
    PERFORM pg_prewarm('ch_a');
    PERFORM pg_prewarm('ch_b');
    PERFORM pg_prewarm('ch_c');
  END LOOP;
END $$;
-- After all the churn, every row in every relation must still be correct.
SELECT count(*) AS bad_a FROM ch_a WHERE payload <> 'a-' || md5(id::text);
SELECT count(*) AS bad_b FROM ch_b WHERE payload <> 'b-' || md5(id::text);
SELECT count(*) AS bad_c FROM ch_c WHERE payload <> 'c-' || md5(id::text);

DROP TABLE ch_a, ch_b, ch_c;
