-- BARK GIN-interface bitmap scans: the k-way merge of the boundaries' TID
-- streams and the required-key skip.  Every query compares a forced bitmap
-- scan of each BARK index with a sequential scan and with a GIN index.
CREATE EXTENSION IF NOT EXISTS bark_multikey;
SELECT setseed(0.7);
-- Skewed elements: 0 is in most rows (POSTING entries after the build),
-- 150 and up in a few.
CREATE TABLE gm (id int PRIMARY KEY, g int, a int4[]);
INSERT INTO gm SELECT i, i % 3,
  CASE WHEN i % 97 = 0 THEN NULL
       WHEN i % 89 = 0 THEN '{}'
       ELSE ARRAY(SELECT floor(200 * random() ^ 3)::int4
                  FROM generate_series(1, 3 + i % 12)) END
FROM generate_series(1, 8000) i;
CREATE INDEX gm_bark ON gm USING bark (a bark_gin_int4_array_ops);
CREATE INDEX gm_bark11 ON gm USING bark (a bark_gin11_int4_array_ops);
-- The column after a key column: a point's TIDs are not in TID order.
CREATE INDEX gm_bark_g ON gm USING bark (g, a bark_gin_int4_array_ops);
CREATE INDEX gm_gin ON gm USING gin (a);
SELECT bark_index_check('gm_bark'), bark_index_check('gm_bark11'),
       bark_index_check('gm_bark_g');
CREATE EXTENSION pageinspect;
SELECT count(*) FILTER (WHERE shape = 'POSTING') > 0 AS has_posting
FROM generate_series(1, pg_relation_size('gm_bark') / 8192 - 1) b,
     bark_page_items('gm_bark', b);

-- Rows of a query on table tab through index idx (NULL: seqscan).
CREATE FUNCTION gm_run(tab text, idx text, q text) RETURNS int[]
LANGUAGE plpgsql AS $$
DECLARE r int[];
BEGIN
  PERFORM set_config('enable_seqscan', (idx IS NULL)::text, true);
  PERFORM set_config('enable_bitmapscan', (idx IS NOT NULL)::text, true);
  PERFORM set_config('enable_indexscan', 'off', true);
  IF idx IS NOT NULL THEN
    EXECUTE format('UPDATE pg_index SET indisvalid = (indexrelid = %L::regclass) WHERE indrelid = %L::regclass AND indexrelid <> %L::regclass',
                   idx, tab, tab || '_pkey');
  END IF;
  EXECUTE format('SELECT coalesce(array_agg(id ORDER BY id), ''{}'') FROM %I WHERE %s', tab, q) INTO r;
  EXECUTE format('UPDATE pg_index SET indisvalid = true WHERE indrelid = %L::regclass', tab);
  RETURN r;
END $$;
CREATE TABLE gm_q (q text);
INSERT INTO gm_q SELECT 'a ' || op || ' ' || quote_literal(arg) || '::int4[]'
FROM unnest(ARRAY['&&', '@>', '<@', '=']) op,
     unnest(ARRAY['{190,0}', '{180,0,1,2,3}', '{0,1}', '{150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170}',
                  '{0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40}',
                  '{199,0,1}', '{0,0,1}', '{5000,0}', '{0,NULL}']) arg;
INSERT INTO gm_q VALUES ('a @> ''{190}'' AND a @> ''{0,1}'''),
  ('a && ''{150,160}'' AND a @> ''{0}'''), ('a @> ''{0}'' AND a <@ ''{0,1,2,3,4,5,6,7,8,9}''');
CREATE TABLE gm_res AS
SELECT q, gm_run('gm', NULL, q) AS seq, gm_run('gm', 'gm_bark', q) AS bark,
       gm_run('gm', 'gm_bark11', q) AS bark11, gm_run('gm', 'gm_bark_g', q) AS bark_g,
       gm_run('gm', 'gm_gin', q) AS gin
FROM gm_q;
SELECT q, cardinality(seq) AS n, bark = seq AS bark_ok, bark11 = seq AS bark11_ok,
       bark_g = seq AS bark_g_ok, gin = seq AS gin_ok
FROM gm_res ORDER BY q;

-- After DELETE, INSERT (LIST entries) and VACUUM.
DELETE FROM gm WHERE id % 5 = 0;
INSERT INTO gm SELECT i, i % 3, ARRAY[0, i % 7, 150 + i % 41]
FROM generate_series(8001, 9000) i;
VACUUM gm;
SELECT bark_index_check('gm_bark'), bark_index_check('gm_bark_g');
SELECT count(*) FILTER (WHERE gm_run('gm', 'gm_bark', q) = gm_run('gm', NULL, q)) AS bark_ok,
       count(*) FILTER (WHERE gm_run('gm', 'gm_bark11', q) = gm_run('gm', NULL, q)) AS bark11_ok,
       count(*) FILTER (WHERE gm_run('gm', 'gm_bark_g', q) = gm_run('gm', NULL, q)) AS bark_g_ok,
       count(*) FILTER (WHERE gm_run('gm', 'gm_gin', q) = gm_run('gm', NULL, q)) AS gin_ok,
       count(*) AS total
FROM gm_q;

-- Full text search: core's tsvector support functions, whose extractQuery
-- returns point keys and partial-match (prefix) keys in one query, merged
-- as one more stream each, and for a query that needs no lexeme (a NOT)
-- every key.  gin_cmp_tslexeme's order is gin_cmp_prefix's.
CREATE OPERATOR FAMILY bark_gin_tsvector_ops USING bark;
CREATE OPERATOR CLASS bark_gin_tsvector_ops
FOR TYPE tsvector USING bark FAMILY bark_gin_tsvector_ops AS
    OPERATOR 1 @@ (tsvector, tsquery),
    FUNCTION 1 (tsvector, tsvector) gin_cmp_tslexeme(text, text),
    FUNCTION 7 gin_extract_tsvector(tsvector, internal, internal),
    FUNCTION 10 gin_extract_tsquery(tsvector, internal, int2, internal,
                                    internal, internal, internal),
    FUNCTION 11 gin_tsquery_consistent(internal, int2, tsvector, int4,
                                       internal, internal, internal, internal),
    FUNCTION 12 gin_cmp_prefix(text, text, int2, internal),
    FUNCTION 13 gin_tsquery_triconsistent(internal, int2, tsvector, int4,
                                          internal, internal, internal),
    STORAGE text;
CREATE TABLE gt (id int PRIMARY KEY, v tsvector);
INSERT INTO gt SELECT i, to_tsvector('simple',
  (SELECT string_agg(w, ' ') FROM (SELECT (ARRAY['apple', 'apricot', 'banana',
     'band', 'bandit', 'cherry', 'cat', 'catalog', 'dog', 'zebra'])[1 + floor(10 * random() ^ 2)::int]
     || CASE WHEN random() < 0.1 THEN (i % 50)::text ELSE '' END AS w
   FROM generate_series(1, 2 + i % 6)) s))
FROM generate_series(1, 4000) i;
INSERT INTO gt VALUES (4001, ''), (4002, NULL);
CREATE INDEX gt_bark ON gt USING bark (v bark_gin_tsvector_ops);
CREATE INDEX gt_gin ON gt USING gin (v);
SELECT bark_index_check('gt_bark');
CREATE TABLE gt_q (q text);
INSERT INTO gt_q SELECT format('v @@ %L::tsquery', t) FROM unnest(ARRAY[
  'apple', 'zebra & apple', 'ban:* & cherry', 'ban:* | cat:*', 'apple & ban:* & cat:* & dog',
  'zebra & !apple', '!apple', 'ap:* & !apricot', 'apple <-> banana', 'cat:* & zebra1:*',
  'nomatch:*', 'zebra & nomatch', 'b:* & c:* & d:*',
  'apple:D | banana:D', 'ban:* | cherry:D']) t;
SELECT q, cardinality(gm_run('gt', NULL, q)) AS n,
       gm_run('gt', 'gt_bark', q) = gm_run('gt', NULL, q) AS bark_ok,
       gm_run('gt', 'gt_gin', q) = gm_run('gt', NULL, q) AS gin_ok
FROM gt_q ORDER BY q;

DROP TABLE gm, gm_q, gm_res, gt, gt_q;
DROP FUNCTION gm_run(text, text, text);
DROP OPERATOR FAMILY bark_gin_tsvector_ops USING bark;
DROP EXTENSION bark_multikey;
DROP EXTENSION pageinspect;
