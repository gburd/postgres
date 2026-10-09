-- pgstatbarkindex: the statistics pgstatindex gives for btree, for BARK.

-- Distinct keys: every entry is a SINGLE, and the page counts add up to
-- the index's size.  The same data in a btree index has as many leaves,
-- give or take the extra bytes a BARK page keeps.
CREATE TABLE bark_stat (a int8);
INSERT INTO bark_stat SELECT g FROM generate_series(1, 10000) g;
CREATE INDEX bark_stat_bark ON bark_stat USING bark (a);
CREATE INDEX bark_stat_btree ON bark_stat USING btree (a);
SELECT version, tree_level, internal_pages, empty_pages, deleted_pages,
       overflow_pages, leaf_fragmentation,
       single_entries, list_entries, posting_entries, oversized_entries,
       index_size = pg_relation_size('bark_stat_bark') AS size_ok,
       1 + internal_pages + leaf_pages + empty_pages + deleted_pages + overflow_pages =
         pg_relation_size('bark_stat_bark') / current_setting('block_size')::int
         AS pages_ok,
       root_block_no > 0 AS root_ok
  FROM pgstatbarkindex('bark_stat_bark');
SELECT abs(b.leaf_pages - t.leaf_pages) <= 1 AS leaves_like_btree,
       abs(b.avg_leaf_density - t.avg_leaf_density) < 2 AS density_like_btree
  FROM pgstatbarkindex('bark_stat_bark') b, pgstatindex('bark_stat_btree') t;

-- Coalesced keys: CREATE INDEX turns a key with a few scattered rows into a
-- LIST and a long run into a POSTING.
CREATE TABLE bark_stat2 (k int4);
INSERT INTO bark_stat2 SELECT 2 FROM generate_series(1, 3000);
INSERT INTO bark_stat2 SELECT CASE WHEN g % 100 = 0 THEN 1 ELSE g + 10 END
  FROM generate_series(1, 2000) g;
CREATE INDEX bark_stat2_k ON bark_stat2 USING bark (k);
SELECT tree_level, single_entries, list_entries, posting_entries, oversized_entries
  FROM pgstatbarkindex('bark_stat2_k');

-- OVERSIZED entries, each with its chain of overflow pages.  The keys are
-- incompressible (a chain of md5s) and larger than a page.
CREATE TABLE bark_stat3 (k text);
INSERT INTO bark_stat3 SELECT 'small' || g FROM generate_series(1, 3) g;
INSERT INTO bark_stat3
  SELECT (SELECT string_agg(md5(s::text || g::text), '') FROM generate_series(1, 300) g)
  FROM generate_series(1, 2) s;
CREATE INDEX bark_stat3_k ON bark_stat3 USING bark (k);
SELECT leaf_pages, overflow_pages, single_entries, oversized_entries
  FROM pgstatbarkindex('bark_stat3_k');

-- VACUUM deletes the leaves it empties.  A temp table keeps the effects of
-- VACUUM predictable.
CREATE TEMP TABLE bark_stat4 AS SELECT g::int8 AS a FROM generate_series(1, 10000) g;
CREATE INDEX bark_stat4_a ON bark_stat4 USING bark (a);
DELETE FROM bark_stat4 WHERE a BETWEEN 1000 AND 5000;
VACUUM bark_stat4;
SELECT deleted_pages > 0 AS has_deleted, single_entries,
       1 + internal_pages + leaf_pages + empty_pages + deleted_pages + overflow_pages =
         pg_relation_size('bark_stat4_a') / current_setting('block_size')::int
         AS pages_ok
  FROM pgstatbarkindex('bark_stat4_a');

-- Only BARK indexes; pgstatindex still takes only btree.
SELECT pgstatbarkindex('bark_stat_btree');
SELECT pgstatbarkindex('bark_stat');
SELECT pgstatindex('bark_stat_bark');

-- Privileges: as pgstatindex, through pg_stat_scan_tables.
CREATE ROLE regress_bark_stat;
SET ROLE regress_bark_stat;
SELECT pgstatbarkindex('bark_stat_bark');
RESET ROLE;
GRANT pg_stat_scan_tables TO regress_bark_stat;
SET ROLE regress_bark_stat;
SELECT single_entries FROM pgstatbarkindex('bark_stat_bark');
RESET ROLE;
DROP ROLE regress_bark_stat;

DROP TABLE bark_stat, bark_stat2, bark_stat3, bark_stat4;
