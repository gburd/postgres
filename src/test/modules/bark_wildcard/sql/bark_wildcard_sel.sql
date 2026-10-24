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

-- More quals, each against its count: a value past every sample key, a
-- path with no keys (an empty point, estimated at one row), a narrow
-- range that runs past the four leaves an exact count reads
-- (extrapolated), and an OR, which the planner estimates per arm.
INSERT INTO wc_sel_q VALUES
  (8, 'doc #> ''["p3", 99999]'''),
  (9, 'doc #= ''["zz", 1]'''),
  (10, 'doc #>= ''["p3", 2]'' AND doc #<= ''["p3", 3]'''),
  (11, 'doc #= ''["p2", true]'' OR doc #= ''["p3", 7]''');
SELECT n, q, act, est BETWEEN act / 3.0 AND act * 3.0 AS within_3x
  FROM wc_sel_q, LATERAL wc_est('wc_sel', q) WHERE n > 7 ORDER BY n;

-- Quals the index does not answer keep the default: a constant that is
-- NULL, a non-constant argument, the operator with the column on the
-- right, and a path the include projection drops (procedure 8 then asks
-- for the NULL entries, which hold more than the matches).
SELECT (wc_est('wc_sel', 'doc #= NULL')).est;
SELECT (wc_est('wc_sel', 'doc #= jsonb_build_array(''p2'', id > 0)')).est;
SELECT (wc_est('wc_sel', '''{"p2": true}''::jsonb #= jsonb_build_array(''p2'', id > 0)')).est;
CREATE TABLE wc_inc (id int, doc jsonb) WITH (autovacuum_enabled = off);
INSERT INTO wc_inc SELECT * FROM wc_sel;
CREATE INDEX wc_inc_doc ON wc_inc USING bark (doc bark_jsonb_wildcard_ops (include = 'id'));
ANALYZE wc_inc;
SELECT (wc_est('wc_inc', 'doc #= ''["p2", true]''')).est;
SELECT n, q, act, est BETWEEN act / 3.0 AND act * 3.0 AS within_3x
  FROM wc_sel_q, LATERAL wc_est('wc_inc', q) WHERE n = 7;
DROP TABLE wc_inc;

-- Keys too long for a leaf live on overflow chains, and so do the
-- separators between them, which the sample leaves out; a narrow range
-- among them still lands within 3x.
CREATE TABLE wc_big (id int, doc jsonb) WITH (autovacuum_enabled = off);
INSERT INTO wc_big
  SELECT g, jsonb_build_object('k', lpad(g::text, 6, '0') || repeat('x', 3000))
    FROM generate_series(1, 2000) g;
CREATE INDEX wc_big_doc ON wc_big USING bark (doc bark_jsonb_wildcard_ops);
ANALYZE wc_big;
SELECT est BETWEEN act / 3.0 AND act * 3.0 AS within_3x, act
  FROM wc_est('wc_big', format('doc #< %L', jsonb_build_array('k', '000400')));
DROP TABLE wc_big;

-- Keys that do not compress are OVERSIZED, separators too, so the sample
-- has no keys at all.  That says nothing of the distribution: a range or
-- point counted exactly within four leaves keeps its count, and a wider
-- one, #? 'k' here, keeps the default, 1% of 600 rows.
CREATE TABLE wc_wide (id int, doc jsonb) WITH (autovacuum_enabled = off);
INSERT INTO wc_wide
  SELECT g, jsonb_build_object('k', lpad(g::text, 6, '0') ||
           (SELECT string_agg(md5(g::text || i), '') FROM generate_series(1, 100) i))
    FROM generate_series(1, 600) g;
CREATE INDEX wc_wide_doc ON wc_wide USING bark (doc bark_jsonb_wildcard_ops);
ANALYZE wc_wide;
SELECT est, act
  FROM wc_est('wc_wide', format('doc #< %L', jsonb_build_array('k', '000300')));
SELECT est, act
  FROM wc_est('wc_wide', format('doc #= %L',
                                (SELECT jsonb_build_array('k', doc ->> 'k')
                                   FROM wc_wide WHERE id = 300)));
SELECT est, act FROM wc_est('wc_wide', 'doc #? ''k''');
DROP TABLE wc_wide;

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

-- A user who may not read every row (here, under row-level security)
-- gets the default too, as jsonb_cmp is not leakproof: the index's counts
-- would tell them how many rows the policy hides.  The policy passes every row, so its own selectivity
-- is about 1: 500 rows is the default, 90000 would be the index's count.
CREATE ROLE regress_wc_sel;
GRANT SELECT ON wc_sel TO regress_wc_sel;
ALTER TABLE wc_sel ENABLE ROW LEVEL SECURITY;
CREATE POLICY wc_sel_few ON wc_sel TO regress_wc_sel USING (id > 0);
SET ROLE regress_wc_sel;
SELECT (wc_est('wc_sel', 'doc #= ''["p1", true]''')).est;
RESET ROLE;
ALTER TABLE wc_sel DISABLE ROW LEVEL SECURITY;
DROP POLICY wc_sel_few ON wc_sel;
REVOKE SELECT ON wc_sel FROM regress_wc_sel;
DROP ROLE regress_wc_sel;

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
