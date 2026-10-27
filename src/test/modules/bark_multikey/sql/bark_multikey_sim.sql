-- An array similarity class after RUM's rum_anyarray_ops: a % q when the
-- cosine similarity |a & q| / sqrt(|a| |q|) is at least 0.5, and a <=> q,
-- 1 minus it.  BARK serves % through GIN's interface, by bitmap and index
-- scans; ORDER BY a <=> q sorts their rows (BARK-Design.mediawiki, "As
-- built (M2b part 2)").  Every query is compared with a Sort by the exact
-- distance over a sequential scan, before and after DML.
CREATE EXTENSION IF NOT EXISTS bark_multikey;
SELECT opcname, amvalidate(oid) FROM pg_opclass
  WHERE opcname LIKE 'bark_int4_array_sim%' ORDER BY opcname;
SELECT '{1,2,3,4}'::int4[] <=> '{1,2}', '{1,2,3,4}'::int4[] % '{1,2}',
       '{1,2,3,4}'::int4[] % '{1}', '{1,1,NULL,2}'::int4[] <=> '{2,1}',
       '{}'::int4[] <=> '{}', '{}'::int4[] % '{}', '{NULL}'::int4[] % '{NULL}';

SELECT setseed(0.17);
CREATE TABLE sm (id int, a int4[]);
CREATE FUNCTION sm_rand_array() RETURNS int4[] LANGUAGE sql VOLATILE AS
  $$SELECT ARRAY(SELECT (random() * 40)::int4
                 FROM generate_series(1, 1 + (random() * 7)::int))$$;
INSERT INTO sm SELECT i,
  CASE WHEN i % 61 = 0 THEN NULL
       WHEN i % 59 = 0 THEN '{}'
       WHEN i % 53 = 0 THEN sm_rand_array() || NULL::int4
       ELSE sm_rand_array() END
FROM generate_series(1, 6000) i;
CREATE INDEX sm_sim ON sm USING bark (a bark_int4_array_sim_ops);
CREATE INDEX sm_sim11 ON sm USING bark (a bark_int4_array_sim11_ops);
CREATE INDEX sm_simc ON sm USING bark (a bark_int4_array_sim_ops, cardinality(a));
SELECT bark_index_check('sm_sim'), bark_index_check('sm_sim11'),
       bark_index_check('sm_simc');

-- The rows a % q through index idx (NULL: a seqscan) by a bitmap or an
-- index scan, ordered by the exact distance a <=> q, then id, the first
-- lim (NULL: all).  The plan must scan idx.
CREATE FUNCTION sm_run(idx text, mode text, q int4[], lim int DEFAULT NULL)
RETURNS int[] LANGUAGE plpgsql AS $$
DECLARE r int[]; s text; plan text := ''; l text;
BEGIN
  PERFORM set_config('enable_seqscan', (idx IS NULL)::text, true);
  PERFORM set_config('enable_bitmapscan', (mode = 'bitmap')::text, true);
  PERFORM set_config('enable_indexscan', (mode = 'index')::text, true);
  PERFORM set_config('enable_indexonlyscan', 'off', true);
  IF idx IS NOT NULL THEN
    EXECUTE format('UPDATE pg_index SET indisvalid = (indexrelid = %L::regclass) WHERE indrelid = ''sm''::regclass', idx);
  END IF;
  s := format('SELECT coalesce(array_agg(id), ''{}'') FROM (SELECT id FROM sm WHERE a %% %L::int4[] ORDER BY a <=> %L::int4[], id LIMIT %s) s',
              q, q, coalesce(lim::text, 'ALL'));
  IF idx IS NOT NULL THEN
    FOR l IN EXECUTE 'EXPLAIN (COSTS OFF) ' || s LOOP
      plan := plan || l || E'\n';
    END LOOP;
    IF position(CASE mode WHEN 'bitmap' THEN 'Bitmap Index Scan on ' || idx || E'\n'
                          ELSE 'Index Scan using ' || idx || ' ' END IN plan) = 0 THEN
      RAISE EXCEPTION 'no % scan of % for %: %', mode, idx, q, plan;
    END IF;
  END IF;
  EXECUTE s INTO r;
  UPDATE pg_index SET indisvalid = true WHERE indrelid = 'sm'::regclass;
  RETURN r;
END $$;

-- Queries: rows' own arrays (with and without their NULL element), random
-- arrays of 1 to 12 elements, repeats, the empty and NULL-only arrays.
CREATE TABLE sm_q (q int4[]);
INSERT INTO sm_q SELECT a FROM sm WHERE id IN (7, 106, 159, 2000, 3233, 5555) AND a IS NOT NULL;
INSERT INTO sm_q SELECT ARRAY(SELECT (random() * 40)::int4
                              FROM generate_series(1, 1 + i % 12))
  FROM generate_series(1, 18) i;
INSERT INTO sm_q VALUES ('{3}'), ('{3,3,3}'), ('{3,NULL}'), ('{}'), ('{NULL}'),
  ('{100,200}'), ('{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20}');

-- For each query: the matching rows, and whether every index and scan
-- returned the seqscan's rows in its order, whole and under LIMIT 1 and 7.
CREATE FUNCTION sm_check() RETURNS TABLE (q int4[], n int, ok bool)
LANGUAGE plpgsql AS $$
DECLARE all_r int[]; lim1 int[]; lim7 int[];
BEGIN
  FOR q IN SELECT sm_q.q FROM sm_q ORDER BY sm_q.q LOOP
    all_r := sm_run(NULL, NULL, q);
    lim1 := sm_run(NULL, NULL, q, 1);
    lim7 := sm_run(NULL, NULL, q, 7);
    n := cardinality(all_r);
    SELECT bool_and(sm_run(i, m, q) = all_r AND sm_run(i, m, q, 1) = lim1 AND
                    sm_run(i, m, q, 7) = lim7)
      INTO ok
      FROM unnest('{sm_sim,sm_sim11,sm_simc}'::text[]) i,
           unnest('{bitmap,index}'::text[]) m;
    RETURN NEXT;
  END LOOP;
END $$;
SELECT * FROM sm_check();
-- The distances themselves, against a seqscan's.
SELECT bool_and(d.ok) FROM sm_q, LATERAL (
  SELECT (SELECT array_agg(a <=> sm_q.q ORDER BY a <=> sm_q.q, id) FROM sm WHERE a % sm_q.q)
         IS NOT DISTINCT FROM
         (SELECT array_agg(a <=> sm_q.q ORDER BY a <=> sm_q.q, id) FROM sm WHERE a <=> sm_q.q <= 0.5) AS ok) d;

-- The plan: a bitmap or index scan for %, a Sort for <=>.  ORDER BY a <=> q
-- without % has no index path.
SET enable_seqscan = off;
EXPLAIN (COSTS OFF)
SELECT id FROM sm WHERE a % '{3,7,9}' ORDER BY a <=> '{3,7,9}' LIMIT 5;
SET enable_bitmapscan = off;
EXPLAIN (COSTS OFF)
SELECT id FROM sm WHERE a % '{3,7,9}' ORDER BY a <=> '{3,7,9}' LIMIT 5;
EXPLAIN (COSTS OFF)
SELECT id FROM sm ORDER BY a <=> '{3,7,9}' LIMIT 5;
RESET enable_bitmapscan;
RESET enable_seqscan;

-- DML: delete, update, insert (LIST entries), vacuum; then again.
DELETE FROM sm WHERE id % 7 = 0;
UPDATE sm SET a = sm_rand_array() WHERE id % 11 = 0;
INSERT INTO sm SELECT i, sm_rand_array() FROM generate_series(6001, 7500) i;
INSERT INTO sm SELECT 7500 + i, a FROM sm WHERE id % 13 = 0 AND id <= 6000;
VACUUM sm;
SELECT bark_index_check('sm_sim'), bark_index_check('sm_sim11'),
       bark_index_check('sm_simc');
SELECT * FROM sm_check();

DROP TABLE sm, sm_q;
DROP FUNCTION sm_run(text, text, int4[], int), sm_check(), sm_rand_array();
DROP EXTENSION bark_multikey;
