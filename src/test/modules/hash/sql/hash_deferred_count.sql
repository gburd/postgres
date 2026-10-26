-- Test the deferred tuple count of hash index inserts.
--
-- An insert that finds the metapage locked leaves its tuple pending instead
-- of waiting for the lock.  The hash-insert-defer-count injection point makes
-- every such insert take that path.
CREATE EXTENSION injection_points;
CREATE EXTENSION pageinspect;
SELECT injection_points_set_local();

CREATE TABLE hash_count_test (v int4) WITH (autovacuum_enabled = false);
CREATE INDEX hash_count_index ON hash_count_test USING hash (v);

-- Without contention, the count is exact
INSERT INTO hash_count_test SELECT g FROM generate_series(1, 10) g;
SELECT ntuples FROM hash_metapage_info(get_raw_page('hash_count_index', 0));

-- Deferred inserts leave the count alone, up to the cap of 32 pending
SELECT injection_points_attach('hash-insert-defer-count', 'notice');
INSERT INTO hash_count_test SELECT g FROM generate_series(1, 20) g;
SELECT ntuples FROM hash_metapage_info(get_raw_page('hash_count_index', 0));

-- Each insert past the cap waits for the lock and counts what is pending
INSERT INTO hash_count_test SELECT g FROM generate_series(1, 13) g;
SELECT ntuples FROM hash_metapage_info(get_raw_page('hash_count_index', 0));
INSERT INTO hash_count_test SELECT g FROM generate_series(1, 40) g;
SELECT ntuples FROM hash_metapage_info(get_raw_page('hash_count_index', 0));

-- A pending count lives on across transactions, and is counted by the next
-- insert that gets the lock
SELECT injection_points_detach('hash-insert-defer-count');
INSERT INTO hash_count_test VALUES (1);
SELECT ntuples FROM hash_metapage_info(get_raw_page('hash_count_index', 0));
SELECT count(*) FROM hash_count_test;

-- Each index has its own pending count
CREATE INDEX hash_count_index2 ON hash_count_test USING hash (v);
SELECT injection_points_attach('hash-insert-defer-count', 'notice');
INSERT INTO hash_count_test SELECT g FROM generate_series(1, 5) g;
SELECT injection_points_detach('hash-insert-defer-count');
INSERT INTO hash_count_test VALUES (1);
SELECT ntuples FROM hash_metapage_info(get_raw_page('hash_count_index', 0));
SELECT ntuples FROM hash_metapage_info(get_raw_page('hash_count_index2', 0));

-- Inserts that add an overflow page wait for the lock, so a deferred count
-- cannot stop the index from splitting.  Build a 2-bucket index, then insert
-- with every lock-free insert deferred.
DROP INDEX hash_count_index, hash_count_index2;
TRUNCATE hash_count_test;
INSERT INTO hash_count_test SELECT 1 FROM generate_series(1, 10);
ANALYZE hash_count_test;
CREATE INDEX hash_count_index ON hash_count_test USING hash (v);
SELECT maxbucket FROM hash_metapage_info(get_raw_page('hash_count_index', 0));
SELECT injection_points_attach('hash-insert-defer-count', 'notice');
INSERT INTO hash_count_test SELECT g FROM generate_series(1, 10000) g;
SELECT injection_points_detach('hash-insert-defer-count');
SELECT maxbucket > 16 AS split,
       ntuples >= 10010 - 32 AND ntuples <= 10010 AS count_ok
  FROM hash_metapage_info(get_raw_page('hash_count_index', 0));

-- Index scans see the deferred tuples
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*) FROM hash_count_test WHERE v = 1;
SELECT count(*) FROM hash_count_test WHERE v = 9999;
RESET enable_seqscan;
RESET enable_bitmapscan;

DROP TABLE hash_count_test;
DROP EXTENSION pageinspect;
DROP EXTENSION injection_points;
