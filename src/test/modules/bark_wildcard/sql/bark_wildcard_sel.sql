-- Selectivity of the wildcard operators (bark_wildcard_sel), read from the
-- index's separators and leaves (bark_estimate_qual_selectivity).
--
-- wc_sel has 100000 documents: p1 is true in 90% of them, p2 in 0.1%, and
-- p3 is a number below 10 in 99% and a large one in 1%.  Each also has a
-- distinct id, so the index is three levels tall.  An estimate counts when
-- it is within 3x of the actual count; plans show the scan it picks.
CREATE EXTENSION bark_wildcard;
SET max_parallel_workers_per_gather = 0;
CREATE TABLE wc_sel (id int, doc jsonb) WITH (autovacuum_enabled = off);
INSERT INTO wc_sel
  SELECT g, jsonb_strip_nulls(jsonb_build_object(
              'id', g,
              'p1', CASE WHEN g % 10 <> 0 THEN true END,
              'p2', CASE WHEN g % 1000 = 0 THEN true END,
              'p3', CASE WHEN g % 100 = 0 THEN g ELSE g % 10 END))
    FROM generate_series(1, 100000) g;
CREATE INDEX wc_sel_doc ON wc_sel USING bark (doc bark_jsonb_wildcard_ops);
ANALYZE wc_sel;

-- The planner's row estimate for a qual on a table, and the actual count.
CREATE FUNCTION wc_est(tab text, q text, OUT est float8, OUT act bigint)
LANGUAGE plpgsql AS $$
DECLARE
  plan json;
BEGIN
  EXECUTE format('EXPLAIN (FORMAT JSON) SELECT * FROM %I WHERE %s', tab, q) INTO plan;
  est := (plan -> 0 -> 'Plan' ->> 'Plan Rows')::float8;
  EXECUTE format('SELECT count(*) FROM %I WHERE %s', tab, q) INTO act;
END $$;

CREATE TABLE wc_sel_q (n int, q text);
INSERT INTO wc_sel_q VALUES
  (1, 'doc #= ''["p1", true]'''),
  (2, 'doc #= ''["p2", true]'''),
  (3, 'doc #? ''p1'''),
  (4, 'doc #? ''p2'''),
  (5, 'doc #< ''["p3", 10]'''),
  (6, 'doc #> ''["p3", 1000]'''),
  (7, 'doc #>= ''["id", 50001]''');
SELECT n, q, act, est BETWEEN act / 3.0 AND act * 3.0 AS within_3x
  FROM wc_sel_q, LATERAL wc_est('wc_sel', q) ORDER BY n;

-- p1 is in most documents, p2 in few: a seqscan for p1, the index for p2.
-- (Before the index read its leaf densities, the wide ranges, #? 'p1' and
-- the 99% head of p3, whose keys share POSTING entries, were estimated
-- from the separators alone and missed by about 10x.)
SELECT a.est = b.est AS same_estimate
  FROM wc_est('wc_sel', 'doc #= ''["p1", true]''') a,
       wc_est('wc_sel', 'doc #= ''["p2", true]''') b;
EXPLAIN (COSTS OFF) SELECT * FROM wc_sel WHERE doc #= '["p1", true]';
EXPLAIN (COSTS OFF) SELECT * FROM wc_sel WHERE doc #= '["p2", true]';
EXPLAIN (COSTS OFF) SELECT * FROM wc_sel WHERE doc #? 'p1';
EXPLAIN (COSTS OFF) SELECT * FROM wc_sel WHERE doc #? 'p2';

-- A one-leaf index cannot say, so the estimate stays the default: 0.5% of
-- 1000 rows for =, though every row matches.
CREATE TABLE wc_small (id int, doc jsonb) WITH (autovacuum_enabled = off);
INSERT INTO wc_small SELECT g, '{"p1": true}' FROM generate_series(1, 1000) g;
CREATE INDEX wc_small_doc ON wc_small USING bark (doc bark_jsonb_wildcard_ops);
ANALYZE wc_small;
SELECT est, act FROM wc_est('wc_small', 'doc #= ''["p1", true]''');

-- A hypothetical index cannot say either: the default again.
SELECT bark_wildcard_hypothetical(true);
SELECT est, act FROM wc_est('wc_sel', 'doc #= ''["p1", true]''');
SELECT bark_wildcard_hypothetical(false);

-- The sample is rebuilt after each invalidation, in a memory context that
-- replaces the old one: a hundred ANALYZEs, each followed by a plan, must
-- not grow the index's relcache memory.
CREATE TEMP TABLE wc_cxt AS
  SELECT bark_wildcard_cxt_size('wc_sel_doc') AS before
    FROM wc_est('wc_sel', 'doc #= ''["p2", true]''');
DO $$
BEGIN
  FOR i IN 1..100 LOOP
    ANALYZE wc_sel;
    PERFORM wc_est('wc_sel', 'doc #= ''["p2", true]''');
  END LOOP;
END $$;
SELECT bark_wildcard_cxt_size('wc_sel_doc') - before AS grown FROM wc_cxt;

DROP TABLE wc_sel, wc_sel_q, wc_small;
DROP FUNCTION wc_est;
DROP EXTENSION bark_wildcard;
