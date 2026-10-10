-- Scans of wildcard indexes.  Every query is answered by a sequential scan
-- and by each wildcard index through an index scan and a bitmap scan; the
-- count, the number of distinct rows and an md5 of the sorted ids must all
-- agree, so a row returned twice, a missing row or a wrong row shows.
-- bark_wildcard.sql left the extensions installed.

-- A value of one of twelve shapes: null, numbers (integers, negatives,
-- quarters, so 1.0 equals 1), strings, booleans, arrays of scalars, arrays
-- holding nested arrays, empty arrays and objects, mixed arrays.
CREATE FUNCTION wc_val(g int) RETURNS jsonb LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE g % 12
    WHEN 0 THEN 'null'::jsonb
    WHEN 1 THEN to_jsonb(g % 23)
    WHEN 2 THEN to_jsonb('s' || g % 17)
    WHEN 3 THEN to_jsonb(g % 3 = 0)
    WHEN 4 THEN jsonb_build_array(g % 23, g * 7 % 23, 's' || g % 5)
    WHEN 5 THEN jsonb_build_array(jsonb_build_array(g % 5, g % 7), g % 11, NULL)
    WHEN 6 THEN '[]'
    WHEN 7 THEN '{}'
    WHEN 8 THEN to_jsonb((g % 40) / 4.0)
    WHEN 9 THEN jsonb_build_array(true, NULL, g % 23, '{}'::jsonb, '[]'::jsonb)
    WHEN 10 THEN jsonb_build_array('s' || g % 17, 's' || g % 13)
    ELSE to_jsonb(-(g % 23))
  END $$;
-- Documents: path a of every shape, or missing; path b an object, an array
-- of objects (b.c and b.d), a value, or missing; a member name with a dot;
-- scalar and array documents (path ""), and NULL documents.
CREATE FUNCTION wc_doc(g int) RETURNS jsonb LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE
    WHEN g % 211 = 0 THEN NULL
    WHEN g % 101 = 0 THEN wc_val(g / 101)
    WHEN g % 103 = 0 THEN jsonb_build_array(wc_val(g), jsonb_build_object('a', wc_val(g + 1)))
    ELSE CASE WHEN g % 5 <> 0 THEN jsonb_build_object('a', wc_val(g)) ELSE '{}' END ||
         CASE g % 4
           WHEN 0 THEN jsonb_build_object('b', jsonb_build_object('c', wc_val(g / 3)))
           WHEN 1 THEN jsonb_build_object('b', jsonb_build_array(
                         jsonb_build_object('c', wc_val(g / 5)),
                         jsonb_build_object('c', wc_val(g / 7), 'd', g % 9)))
           WHEN 2 THEN jsonb_build_object('b', wc_val(g / 11))
           ELSE '{}' END ||
         jsonb_build_object('n', g % 31) ||
         CASE WHEN g % 9 = 0 THEN jsonb_build_object('x.y', g % 4) ELSE '{}' END
  END $$;

-- The same rows in one table per index, so each query can be forced onto
-- one index: no projection, an include list, an exclude list, and a
-- compound (id, doc) index created before the rows were inserted.
CREATE TABLE wc_seq AS SELECT g AS id, wc_doc(g) AS doc FROM generate_series(1, 4000) g;
CREATE TABLE wc_plain AS SELECT * FROM wc_seq;
CREATE INDEX wc_plain_doc ON wc_plain USING bark (doc bark_jsonb_wildcard_ops);
CREATE TABLE wc_inc AS SELECT * FROM wc_seq;
CREATE INDEX wc_inc_doc ON wc_inc USING bark (doc bark_jsonb_wildcard_ops (include = 'a,b.c'));
CREATE TABLE wc_exc AS SELECT * FROM wc_seq;
CREATE INDEX wc_exc_doc ON wc_exc USING bark (doc bark_jsonb_wildcard_ops (exclude = 'b'));
CREATE TABLE wc_tid (id int, doc jsonb);
CREATE INDEX wc_tid_doc ON wc_tid USING bark (id, doc bark_jsonb_wildcard_ops);
INSERT INTO wc_tid SELECT * FROM wc_seq;
ANALYZE wc_seq, wc_plain, wc_inc, wc_exc, wc_tid;

-- wc_run(table, index, qual, mode): count, distinct ids and md5 of the ids
-- of table's rows that satisfy qual, read by a sequential scan ('seq'), an
-- index scan or a bitmap scan of index; 'wrong plan' when the plan does not
-- read that way.
CREATE FUNCTION wc_run(tbl text, idx text, qual text, mode text) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  sql text := format('SELECT count(*) || '':'' || count(DISTINCT id) || '':'' ||
                        coalesce(md5(string_agg(id::text, '','' ORDER BY id)), ''-'')
                      FROM (SELECT id FROM %I WHERE %s) s', tbl, qual);
  plan text;
  r text;
BEGIN
  PERFORM set_config('enable_seqscan', (mode = 'seq')::text, true);
  PERFORM set_config('enable_indexscan', (mode = 'index')::text, true);
  PERFORM set_config('enable_indexonlyscan', 'off', true);
  PERFORM set_config('enable_bitmapscan', (mode = 'bitmap')::text, true);
  FOR plan IN EXECUTE 'EXPLAIN (COSTS OFF) ' || sql LOOP
    IF (mode = 'seq' AND plan LIKE '%Seq Scan on ' || tbl || '%') OR
       (mode = 'index' AND plan LIKE '%Index Scan using ' || idx || ' on ' || tbl || '%') OR
       (mode = 'bitmap' AND plan LIKE '%Bitmap Index Scan on ' || idx || '%') THEN
      EXECUTE sql INTO r;
      RETURN r;
    END IF;
  END LOOP;
  RETURN 'wrong plan';
END $$;

-- wc_compare(qual, oracle): the sequential scan's answer; every index or
-- bitmap answer that differs from it (NULL when none does); and, given an
-- oracle (the same question as a jsonpath in lax mode), 'same' or the
-- oracle's differing answer.
CREATE FUNCTION wc_compare(qual text, oracle text, OUT seq text,
                           OUT mismatches text, OUT jsonpath text)
LANGUAGE plpgsql AS $$
DECLARE
  t text[];
  m text;
  r text;
BEGIN
  seq := wc_run('wc_seq', NULL, qual, 'seq');
  FOREACH t SLICE 1 IN ARRAY ARRAY[['wc_plain', 'wc_plain_doc'], ['wc_inc', 'wc_inc_doc'],
                                   ['wc_exc', 'wc_exc_doc'], ['wc_tid', 'wc_tid_doc']] LOOP
    FOREACH m IN ARRAY ARRAY['index', 'bitmap'] LOOP
      r := wc_run(t[1], t[2], qual, m);
      IF r IS DISTINCT FROM seq THEN
        mismatches := concat_ws(' ', mismatches, t[1] || '/' || m || '=' || r);
      END IF;
    END LOOP;
  END LOOP;
  IF oracle IS NOT NULL THEN
    r := wc_run('wc_seq', NULL, oracle, 'seq');
    jsonpath := CASE WHEN r = seq THEN 'same' ELSE r END;
  END IF;
END $$;

-- The jsonpath of a dotted path without escapes: $."b"."c".
CREATE FUNCTION wc_jpath(p text) RETURNS text LANGUAGE sql IMMUTABLE AS $$
  SELECT '$' || coalesce(string_agg('."' || c || '"', '' ORDER BY i), '')
    FROM unnest(string_to_array(nullif(p, ''), '.')) WITH ORDINALITY u(c, i)
$$;

-- Every comparison operator against a value of every type, on a path of
-- documents (a), on one below arrays of objects (b.c, which wc_exc's
-- projection drops), and on scalar documents (""); #? on every path, on a
-- missing one, and on paths wc_inc drops (b, b.d, n, x\.y).  Scalar
-- comparisons carry the jsonpath oracle: in lax mode, $.a ? (@ == 5) also
-- looks into an array at a, and compares only values of one type, as the
-- class does.  Lax mode also unwraps an array nested in that array, which
-- the class indexes as a value, so the oracle skips array items
-- (@.type() != "array"); the difference itself is shown below.  jsonpath
-- cannot compare arrays or objects, so those have no oracle.
CREATE TABLE wc_quals (n serial, q text, oracle text);
INSERT INTO wc_quals (q, oracle)
SELECT format('doc %s %L', op, jsonb_build_array(p, v)),
       CASE WHEN jsonb_typeof(v) NOT IN ('array', 'object') THEN
         format('doc @? %L', format('%s ? (@.type() != "array" && @ %s %s)',
                                    wc_jpath(p), jop, v)) END
  FROM (VALUES (1, 'a'), (2, 'b.c'), (3, '')) p(po, p),
       (VALUES (1, 'null'::jsonb), (2, '"s5"'), (3, '5'), (4, '2.5'), (5, '-3'),
               (6, 'true'), (7, 'false'), (8, '[1, 3]'), (9, '[]'), (10, '{}')) v(vo, v),
       (VALUES (1, '#<', '<'), (2, '#<=', '<='), (3, '#=', '=='), (4, '#>=', '>='),
               (5, '#>', '>')) o(oo, op, jop)
 WHERE p <> '' OR vo IN (1, 2, 3, 6, 9, 10)
 ORDER BY po, vo, oo;
INSERT INTO wc_quals (q, oracle) VALUES
  ('doc #= ''["b.d", 3]''', 'doc @? ''$."b"."d" ? (@.type() != "array" && @ == 3)'''),
  ('doc #< ''["n", 5]''', 'doc @? ''$."n" ? (@.type() != "array" && @ < 5)'''),
  ('doc #= ''["x\\.y", 2]''', 'doc @? ''$."x.y" ? (@.type() != "array" && @ == 2)'''),
  ('doc #= ''["a", 1.00]''', 'doc @? ''$."a" ? (@.type() != "array" && @ == 1)'''),
  ('doc #= ''["zz", 1]''', NULL),
  ('doc #> ''["a", 3]'' AND doc #< ''["a", 7]''', NULL),
  ('doc #= ANY (ARRAY[''["a", 5]'', ''["a", "s3"]'', ''["b.c", true]'']::jsonb[])', NULL),
  ('id < 1500 AND doc #= ''["a", 5]''', NULL),
  ('id = 404 AND doc #? ''''', NULL),
  ('id IN (5, 10, 15, 20, 101) AND doc #? ''a''', NULL),
  ('id BETWEEN 100 AND 300 AND doc #> ''["b.c", 3]''', NULL),
  ('doc IS NULL', NULL);
INSERT INTO wc_quals (q)
SELECT format('doc #? %L', p)
  FROM unnest(ARRAY['a', 'b', 'b.c', 'b.d', '', 'n', 'x\.y', 'zz', 'a.b']) p;
SELECT n, q, (wc_compare(q, oracle)).* FROM wc_quals ORDER BY n;

-- The plans wc_run accepted, two of them in full: the projected-out path
-- b.c on wc_exc (every row the scan reads is rechecked), and the compound
-- index with a range on its first column.
SET enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT id FROM wc_exc WHERE doc #= '["b.c", 5]';
SET enable_bitmapscan = off;
EXPLAIN (COSTS OFF) SELECT id FROM wc_tid WHERE id < 1500 AND doc #= '["a", 5]';
RESET enable_bitmapscan;
RESET enable_seqscan;

-- Where jsonpath and the class differ: #? says a path holds a value the
-- class indexes, so a path holding only a non-empty object has none, while
-- $.a exists; and an empty array at a is the value [], which a lax-mode
-- filter unwraps to nothing.  Arrays and objects compare in the class (by
-- jsonb_cmp) but not in jsonpath.
SELECT d, d #? 'a' AS "#?", d @? '$.a' AS "@? $.a",
       d @? '$.a ? (@.type() != "object")' AS "@? filtered"
  FROM (VALUES ('{"a": {"b": 1}}'::jsonb), ('{"a": []}'), ('{"a": [[1, 3]]}')) v(d);
SELECT '{"a": [[1, 3]]}'::jsonb #= '["a", 1]' AS "#= 1",
       '{"a": [[1, 3]]}'::jsonb @? '$.a ? (@ == 1)' AS "@? == 1",
       '{"a": [[1, 3]]}'::jsonb #= '["a", [1, 3]]' AS "#= [1, 3]";
-- The rows where the unguarded oracle differs, for one query.
SELECT count(*) FILTER (WHERE doc #= '["a", 3]') AS "#=",
       count(*) FILTER (WHERE doc @? '$.a ? (@ == 3)') AS "@?",
       count(*) FILTER (WHERE doc @? '$.a ? (@ == 3)' AND NOT doc #= '["a", 3]') AS "@? only"
  FROM wc_seq;

-- The indexes are sound, and each holds exactly the (key, heap TID) members
-- bark_wildcard_keys gives the heap's rows under its projection (a row with
-- no kept key, or a NULL document, has a NULL entry), compared both ways,
-- and the markers of the paths that hold an array.  amcheck's
-- heapallindexed refuses an index with an extracted column, so this
-- comparison stands in for it.
SELECT bark_index_check('wc_plain_doc'), bark_index_check('wc_inc_doc'),
       bark_index_check('wc_exc_doc'), bark_index_check('wc_tid_doc');
SELECT bark_index_check('wc_plain_doc', true);
CREATE FUNCTION wc_heap_keys(tbl regclass, include text, exclude text,
                             OUT key jsonb, OUT tid tid, OUT marker bool)
RETURNS SETOF record LANGUAGE plpgsql AS $$
BEGIN
  RETURN QUERY EXECUTE format($q$
    SELECT k, ctid, false FROM %1$s, bark_wildcard_keys(doc, $1, $2) k
     WHERE jsonb_array_length(k) = 2
    UNION ALL
    SELECT NULL, ctid, false FROM %1$s
     WHERE doc IS NULL OR NOT EXISTS (SELECT FROM bark_wildcard_keys(doc, $1, $2) k
                                       WHERE jsonb_array_length(k) = 2)
    UNION
    SELECT k, NULL, true FROM %1$s, bark_wildcard_keys(doc, $1, $2) k
     WHERE jsonb_array_length(k) = 1$q$, tbl) USING include, exclude;
END $$;
CREATE FUNCTION wc_index_keys(idx regclass, OUT key jsonb, OUT tid tid,
                              OUT marker bool)
RETURNS SETOF record LANGUAGE sql AS $$
  SELECT key, CASE WHEN NOT marker THEN tid END, marker
    FROM bark_wildcard_entries(idx)
$$;
SELECT i, (SELECT count(*) FROM wc_index_keys(i::regclass)) AS entries,
       (SELECT count(*) FROM (SELECT * FROM wc_index_keys(i::regclass)
                              EXCEPT ALL SELECT * FROM wc_heap_keys(t::regclass, inc, exc)) x) AS index_only,
       (SELECT count(*) FROM (SELECT * FROM wc_heap_keys(t::regclass, inc, exc)
                              EXCEPT ALL SELECT * FROM wc_index_keys(i::regclass)) x) AS heap_only
  FROM (VALUES ('wc_plain_doc', 'wc_plain', NULL, NULL), ('wc_inc_doc', 'wc_inc', 'a,b.c', NULL),
               ('wc_exc_doc', 'wc_exc', NULL, 'b'), ('wc_tid_doc', 'wc_tid', NULL, NULL)) v(i, t, inc, exc)
 ORDER BY i;

-- A member named "" at the root adds no path segment, so its values are at
-- the paths below it: {"": {"s": 5}} has 5 at s.  Every operator must agree
-- with the index there.  Each query's ids by seqscan and by forced index and
-- bitmap scans, which must be equal.
CREATE TABLE wc_empty (id int, doc jsonb);
INSERT INTO wc_empty VALUES (1, '{"": {"s": 5}}'), (2, '{"s": 5}'),
  (3, '{"": {"s": 6}, "s": 5}'), (4, '{"": 5}'), (5, '{"a": {"": 5}}'),
  (6, '{"a.": 5}'), (7, '5'), (8, '{"": [5, 6]}'), (9, '{"": {"": {"s": 7}}}');
CREATE INDEX wc_empty_doc ON wc_empty USING bark (doc bark_jsonb_wildcard_ops);
CREATE TABLE wc_empty_q AS
  SELECT q FROM unnest(ARRAY['doc #= ''["s", 5]''', 'doc #< ''["s", 7]''',
                             'doc #>= ''["s", 6]''', 'doc #= ''["", 5]''',
                             'doc #= ''["a.", 5]''', 'doc #? ''s''',
                             'doc #? ''''']) q;
SELECT q, wc_run('wc_empty', NULL, q, 'seq') AS seq,
       wc_run('wc_empty', 'wc_empty_doc', q, 'index') AS index,
       wc_run('wc_empty', 'wc_empty_doc', q, 'bitmap') AS bitmap
  FROM wc_empty_q ORDER BY q;
SELECT q, (SELECT array_agg(id ORDER BY id) FROM wc_empty e
            WHERE CASE q WHEN 's=5' THEN doc #= '["s", 5]'
                         WHEN 's?' THEN doc #? 's' END) AS ids
  FROM unnest('{s=5,s?}'::text[]) q;
DROP TABLE wc_empty, wc_empty_q;
DROP TABLE wc_seq, wc_plain, wc_inc, wc_exc, wc_tid, wc_quals;
DROP FUNCTION wc_run, wc_compare, wc_jpath, wc_doc, wc_val, wc_heap_keys, wc_index_keys;
DROP EXTENSION bark_wildcard;
