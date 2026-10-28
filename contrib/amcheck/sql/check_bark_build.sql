--
-- CREATE INDEX on BARK: LIST and POSTING entries formed by the build.
--
-- These cases check the index with bark_index_check() after the changes
-- they make, so they live here rather than in the core regression suite.
-- (The amcheck extension is created by the earlier "check" test.)
--

-- ===========================================================================
-- CREATE INDEX forms LIST and POSTING entries where insert would coalesce.
-- The sort delivers each run of equal keys with its heap TIDs ascending, and
-- the build writes the run as the entries insert would choose, each at most
-- a tenth of a page, as nbtree's build limits its posting lists.  Page counts
-- are compared as booleans with the same rows inserted into an existing
-- index; bark_bld_check compares a query through a bark_bld index with a
-- sequential scan, as bark_sfx_check does.
-- ===========================================================================
CREATE FUNCTION bark_bld_check(q text, bitmap bool DEFAULT false)
RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  plan text;
  viaidx text;
  viaseq text;
BEGIN
  PERFORM set_config('enable_seqscan', 'off', true);
  PERFORM set_config('enable_indexscan', (NOT bitmap)::text, true);
  PERFORM set_config('enable_indexonlyscan', (NOT bitmap)::text, true);
  PERFORM set_config('enable_bitmapscan', bitmap::text, true);
  EXECUTE 'EXPLAIN (COSTS OFF, FORMAT JSON) ' || q INTO plan;
  EXECUTE 'SELECT md5(string_agg(x::text, '' '')) FROM (' || q || ') x'
    INTO viaidx;
  PERFORM set_config('enable_seqscan', 'on', true);
  PERFORM set_config('enable_indexscan', 'off', true);
  PERFORM set_config('enable_indexonlyscan', 'off', true);
  PERFORM set_config('enable_bitmapscan', 'off', true);
  EXECUTE 'SELECT md5(string_agg(x::text, '' '')) FROM (' || q || ') x'
    INTO viaseq;
  IF position('"Index Name": "bark_bld' IN plan) = 0 THEN
    RETURN 'no index';
  END IF;
  RETURN CASE WHEN viaidx IS NOT DISTINCT FROM viaseq THEN 'ok'
              ELSE 'MISMATCH' END;
END $$;

-- One hot key; 1000 keys with each key's rows together (and some NULL keys
-- scattered among them); 1000 keys round-robin, so each key's rows are
-- scattered over the heap.
CREATE TABLE bark_bld_one (k int, v int) WITH (autovacuum_enabled = off);
INSERT INTO bark_bld_one SELECT 1, g FROM generate_series(1, 100000) g;
CREATE TABLE bark_bld_kc (k int, v int) WITH (autovacuum_enabled = off);
INSERT INTO bark_bld_kc
  SELECT CASE WHEN g % 1001 = 0 THEN NULL ELSE (g - 1) / 100 END, g
  FROM generate_series(1, 100000) g;
CREATE TABLE bark_bld_ks (k int, v int) WITH (autovacuum_enabled = off);
INSERT INTO bark_bld_ks SELECT g % 1000, g FROM generate_series(1, 100000) g;
CREATE INDEX bark_bld_one_idx ON bark_bld_one USING bark (k);
CREATE INDEX bark_bld_kc_idx ON bark_bld_kc USING bark (k);
CREATE INDEX bark_bld_ks_idx ON bark_bld_ks USING bark (k);
CREATE TABLE bark_bld_one_r (k int, v int) WITH (autovacuum_enabled = off);
CREATE INDEX bark_bld_one_r_idx ON bark_bld_one_r USING bark (k);
INSERT INTO bark_bld_one_r SELECT * FROM bark_bld_one;
CREATE TABLE bark_bld_kc_r (k int, v int) WITH (autovacuum_enabled = off);
CREATE INDEX bark_bld_kc_r_idx ON bark_bld_kc_r USING bark (k);
INSERT INTO bark_bld_kc_r SELECT * FROM bark_bld_kc;
CREATE TABLE bark_bld_ks_r (k int, v int) WITH (autovacuum_enabled = off);
CREATE INDEX bark_bld_ks_r_idx ON bark_bld_ks_r USING bark (k);
INSERT INTO bark_bld_ks_r SELECT * FROM bark_bld_ks;
SELECT pg_relation_size('bark_bld_one_idx') <=
       pg_relation_size('bark_bld_one_r_idx') * 1.5 AS one_like_insert,
       pg_relation_size('bark_bld_kc_idx') <=
       pg_relation_size('bark_bld_kc_r_idx') * 1.5 AS kc_like_insert,
       pg_relation_size('bark_bld_ks_idx') <=
       pg_relation_size('bark_bld_ks_r_idx') * 1.5 AS ks_like_insert;
DROP TABLE bark_bld_one_r, bark_bld_kc_r, bark_bld_ks_r;
SELECT bark_index_check('bark_bld_one_idx');
SELECT bark_index_check('bark_bld_kc_idx');
SELECT bark_index_check('bark_bld_ks_idx');
-- A parallel build reads the same sorted stream, so it writes the same tree.
SET min_parallel_table_scan_size = 0;
SET max_parallel_maintenance_workers = 4;
CREATE INDEX bark_bld_ks_par ON bark_bld_ks USING bark (k);
RESET min_parallel_table_scan_size;
RESET max_parallel_maintenance_workers;
SELECT pg_relation_size('bark_bld_ks_par') =
       pg_relation_size('bark_bld_ks_idx') AS parallel_same_size;
SELECT bark_index_check('bark_bld_ks_par');
DROP INDEX bark_bld_ks_par;

-- Runs longer than one entry.  The hot key's rows fill many POSTING
-- entries.  Here each heap page holds about one row of a key, so the set
-- stays a LIST, and 400 rows of a key take three of them.  A key too wide
-- for a LIST of two stays SINGLE, and a narrower wide key forms a short LIST.
CREATE TABLE bark_bld_sparse (k int, v int, pad text) WITH (autovacuum_enabled = off);
INSERT INTO bark_bld_sparse SELECT g % 20, g, repeat('x', 400)
  FROM generate_series(1, 8000) g;
CREATE INDEX bark_bld_sparse_idx ON bark_bld_sparse USING bark (k);
SELECT bark_index_check('bark_bld_sparse_idx');
CREATE TABLE bark_bld_wkey (t text) WITH (autovacuum_enabled = off);
ALTER TABLE bark_bld_wkey ALTER t SET STORAGE plain;
INSERT INTO bark_bld_wkey
  SELECT lpad(n::text, 4, '0') ||
         (SELECT string_agg(md5(n::text || i::text), '')
          FROM generate_series(1, CASE WHEN n % 2 = 0 THEN 28 ELSE 12 END) i)
  FROM generate_series(1, 60) n, generate_series(1, 10) r;
CREATE INDEX bark_bld_wkey_idx ON bark_bld_wkey USING bark (t);
SELECT bark_index_check('bark_bld_wkey_idx');

-- Keys on both sides of the oversize threshold.  The build forms a row's
-- tuple to measure it only when a bound on its size says it may be
-- oversized; in-line incompressible keys (STORAGE plain) of 2660-2700 bytes
-- with an int8 INCLUDE column straddle the threshold, so a bound that
-- undercounts a key's length, header or alignment sends an oversized row
-- through the sort and the loader, which the check rejects.
CREATE TABLE bark_bld_edge (t text, i int8) WITH (autovacuum_enabled = off);
ALTER TABLE bark_bld_edge ALTER t SET STORAGE plain;
INSERT INTO bark_bld_edge
  SELECT substr(lpad(n::text, 4, '0') ||
                (SELECT string_agg(md5(n::text || i::text), '')
                 FROM generate_series(1, 85) i), 1, 2660 + n), n
  FROM generate_series(0, 40) n;
INSERT INTO bark_bld_edge VALUES (NULL, 100), ('short', NULL);
CREATE INDEX bark_bld_edge_idx ON bark_bld_edge USING bark (t) INCLUDE (i);
SELECT bark_index_check('bark_bld_edge_idx');

CREATE TABLE bark_bld_queries (n int, q text, bitmap bool);
INSERT INTO bark_bld_queries VALUES
  (1, 'SELECT k, count(*) FROM bark_bld_one WHERE k >= 0 GROUP BY k', false),
  (2, 'SELECT count(*), sum(v) FROM bark_bld_one WHERE k = 1', true),
  (3, 'SELECT k, count(*) FROM bark_bld_kc WHERE k >= 0 GROUP BY k ORDER BY k', false),
  (4, 'SELECT count(*) FROM bark_bld_kc WHERE k IS NULL', false),
  (5, 'SELECT count(*), sum(v) FROM bark_bld_kc WHERE k BETWEEN 100 AND 199', true),
  (6, 'SELECT count(*), sum(v) FROM bark_bld_kc WHERE k IS NULL', true),
  (7, 'SELECT k, count(*) FROM bark_bld_ks WHERE k >= 0 GROUP BY k ORDER BY k', false),
  (8, 'SELECT k FROM bark_bld_ks WHERE k BETWEEN 10 AND 12 ORDER BY k DESC', false),
  (9, 'SELECT count(*), sum(v) FROM bark_bld_ks WHERE k BETWEEN 100 AND 199', true),
  (10, 'SELECT k, count(*) FROM bark_bld_sparse WHERE k >= 0 GROUP BY k ORDER BY k', false),
  (11, 'SELECT count(*), sum(length(pad)) FROM bark_bld_sparse WHERE k = 7', true),
  (12, 'SELECT left(t, 4), length(t), count(*) FROM bark_bld_wkey WHERE t >= ''0020'' GROUP BY t ORDER BY t', false),
  (13, 'SELECT count(*) FROM bark_bld_wkey WHERE t >= ''0020''', true),
  (14, 'SELECT left(t, 4), length(t), i FROM bark_bld_edge WHERE t >= ''0'' ORDER BY t', false),
  (15, 'SELECT count(*), sum(i) FROM bark_bld_edge WHERE t >= ''0''', true);
SELECT n, bark_bld_check(q, bitmap) FROM bark_bld_queries ORDER BY n;

-- VACUUM after the build.  Built POSTING entries carry the removal reserve
-- (bark_form_posting sizes them for it), so VACUUM rewrites every entry in
-- place; amcheck checks the reserve and the item ceiling.
DELETE FROM bark_bld_one WHERE v % 3 = 0;
DELETE FROM bark_bld_kc WHERE v % 3 = 0;
DELETE FROM bark_bld_ks WHERE v % 3 = 0;
DELETE FROM bark_bld_sparse WHERE v % 3 = 0;
VACUUM bark_bld_one, bark_bld_kc, bark_bld_ks, bark_bld_sparse;
SELECT bark_index_check('bark_bld_one_idx');
SELECT bark_index_check('bark_bld_kc_idx');
SELECT bark_index_check('bark_bld_ks_idx');
SELECT bark_index_check('bark_bld_sparse_idx');
SELECT n, bark_bld_check(q, bitmap) FROM bark_bld_queries WHERE n <= 11 ORDER BY n;
-- Insert then grows the built entries, with the new rows' TIDs landing in
-- the space VACUUM freed, inside the existing sets.
INSERT INTO bark_bld_one SELECT 1, -g FROM generate_series(1, 20000) g;
INSERT INTO bark_bld_ks SELECT g % 1000, -g FROM generate_series(1, 20000) g;
SELECT bark_index_check('bark_bld_one_idx');
SELECT bark_index_check('bark_bld_ks_idx');
SELECT n, bark_bld_check(q, bitmap) FROM bark_bld_queries
  WHERE n IN (1, 2, 7, 8, 9) ORDER BY n;
DROP TABLE bark_bld_queries;
DROP TABLE bark_bld_one, bark_bld_kc, bark_bld_ks, bark_bld_sparse, bark_bld_wkey,
  bark_bld_edge;

-- Where insert does not coalesce, neither does the build.  A unique index
-- holds equal keys only when they are NULL, and builds the same tree as an
-- index whose INCLUDE column (here also NULL, so every tuple has the same
-- size) rules coalescing out; a plain index on the same rows coalesces
-- them.  An INCLUDE index returns each row's own payload.
CREATE TABLE bark_bld_nc (k int, z int, p text) WITH (autovacuum_enabled = off);
INSERT INTO bark_bld_nc SELECT NULL, NULL, 'row' || g FROM generate_series(1, 20000) g;
CREATE UNIQUE INDEX bark_bld_nc_u ON bark_bld_nc USING bark (k);
CREATE INDEX bark_bld_nc_inc ON bark_bld_nc USING bark (k) INCLUDE (z);
CREATE INDEX bark_bld_nc_plain ON bark_bld_nc USING bark (k);
SELECT pg_relation_size('bark_bld_nc_u') =
       pg_relation_size('bark_bld_nc_inc') AS unique_single_only,
       pg_relation_size('bark_bld_nc_plain') * 4 <
       pg_relation_size('bark_bld_nc_u') AS plain_coalesced;
SELECT bark_index_check('bark_bld_nc_u');
SELECT bark_index_check('bark_bld_nc_plain');
TRUNCATE bark_bld_nc;
DROP INDEX bark_bld_nc_u, bark_bld_nc_inc, bark_bld_nc_plain;
INSERT INTO bark_bld_nc SELECT 1, NULL, 'row' || g FROM generate_series(1, 5) g;
CREATE INDEX bark_bld_nc_idx ON bark_bld_nc USING bark (k) INCLUDE (p);
SET enable_seqscan = off;
SET enable_bitmapscan = off;
EXPLAIN (COSTS OFF) SELECT k, p FROM bark_bld_nc WHERE k = 1;
SELECT k, p FROM bark_bld_nc WHERE k = 1 ORDER BY p;
RESET enable_seqscan;
RESET enable_bitmapscan;
DROP TABLE bark_bld_nc;
DROP FUNCTION bark_bld_check(text, bool);
