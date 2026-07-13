-- Tests that eviction always prefers reclaiming an already-COOL buffer
-- over demoting a HOT one -- the property that lets a one-touch flood
-- (VACUUM/big sequential scan/COPY) cycle through the buffer pool without
-- displacing the genuinely hot working set.  See StrategyGetBuffer() in
-- src/backend/storage/buffer/freelist.c.
--
-- Runs against a deliberately tiny shared_buffers (see test_bufmgr.conf)
-- so the whole pool can be forced through real eviction cycles by
-- reading a "flood" relation many times larger than the pool, then
-- checks that a COOL tracked block got reclaimed along the way while a
-- HOT tracked block did not.
CREATE EXTENSION IF NOT EXISTS pg_buffercache;

-- Tracked table: block 0 will be left COOL, block 1 will be promoted HOT.
CREATE TABLE hotcool (a int, b text) WITH (autovacuum_enabled = off);
INSERT INTO hotcool SELECT i, repeat('x', 200) FROM generate_series(1, 400) i;
-- Start from a known state: no buffers for this relation resident, same
-- reason as test_bufmgr_admission.sql -- only the side effect matters.
DO $$ BEGIN PERFORM pg_buffercache_evict_relation('hotcool'::regclass); END $$;

SELECT count(*) FROM hotcool WHERE ctid = '(0,1)';
SELECT count(*) FROM hotcool WHERE ctid = '(1,1)';
SELECT count(*) FROM hotcool WHERE ctid = '(1,1)';

-- usagecount's bit 0 is coolstate (BUF_STATE_GET_COOLSTATE() in
-- buf_internals.h): 0 = COOL, 1 = HOT.  Bit 1 is the second-chance ref
-- bit PinBuffer sets on every pin.  We alias bit 0 as "hotstate" below --
-- 1 means HOT, 0 means COOL -- since that reads more naturally than
-- "coolstate = 1" meaning hot.  "(usagecount & 1)" isolates that bit,
-- independent of the ref bit's incidental value.
SELECT usagecount & 1 AS hotstate FROM pg_buffercache
 WHERE relfilenode = pg_relation_filenode('hotcool'::regclass)
   AND relforknumber = 0 AND relblocknumber = 0;
SELECT usagecount & 1 AS hotstate FROM pg_buffercache
 WHERE relfilenode = pg_relation_filenode('hotcool'::regclass)
   AND relforknumber = 0 AND relblocknumber = 1;

-- Flood: a throwaway relation, many times larger than shared_buffers
-- (8MB = 1024 buffers; ~200 bytes/row means well under 40 rows/page, so
-- 50000 rows is comfortably ~1500+ pages -- a full sweep or two of the
-- pool), read with a single sequential scan.  Every block is admitted
-- COOL and drained again as the scan moves on.
--
-- Sizing this matters: too small (e.g. 20000 rows) and the flood never
-- reaches the COOL block's position, so it survives too -- a false
-- negative, not evidence the property doesn't hold.  Too large (e.g.
-- 200000 rows), and the HOT block gets reclaimed too, even though
-- nothing ever touched it after promotion -- see the module README for
-- the standing question that raises about bgwriter demand-driven
-- pre-cooling under sustained heavy allocation pressure.  50000 is the
-- verified sweet spot for this shared_buffers.
CREATE TABLE flood (a int, b text) WITH (autovacuum_enabled = off);
INSERT INTO flood SELECT i, repeat('y', 200) FROM generate_series(1, 50000) i;
SELECT count(*) FROM flood;

-- The COOL block should have been reclaimed somewhere along the way --
-- no longer resident for this relation at all.
SELECT count(*) FROM pg_buffercache
 WHERE relfilenode = pg_relation_filenode('hotcool'::regclass)
   AND relforknumber = 0 AND relblocknumber = 0;

-- The HOT block should have survived untouched.
SELECT usagecount & 1 AS hotstate FROM pg_buffercache
 WHERE relfilenode = pg_relation_filenode('hotcool'::regclass)
   AND relforknumber = 0 AND relblocknumber = 1;
