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

-- A row matches a boundary of one path through at most one entry unless
-- the path, or a path above it, has held an array: procedure 8 then sets
-- BarkQueryFlags.oneentry and the scan keeps no seen set.  Every document
-- here has a scalar at s, x\.y and t.u, an array at arr, and b.c below an
-- array of objects in a third of them; include = 'b.c,s,arr' keeps the
-- marker [b] above b.c, and drops t.u and x\.y, whose scans read every key.
-- One point needs no set anyway; = ANY never takes the flag.  Each answer
-- is compared with a sequential scan's by count, distinct ids and md5.
CREATE FUNCTION wc1_doc(g int) RETURNS jsonb LANGUAGE sql IMMUTABLE AS $$
  SELECT jsonb_build_object('s', g % 23, 't', jsonb_build_object('u', 's' || g % 7),
                            'x.y', g % 5, 'arr', jsonb_build_array(g % 11, g % 13, g % 3)) ||
         CASE WHEN g % 3 = 0
           THEN jsonb_build_object('b', jsonb_build_array(jsonb_build_object('c', g % 4),
                                                          jsonb_build_object('c', g % 6)))
           ELSE jsonb_build_object('b', jsonb_build_object('c', g % 5)) END $$;
CREATE TABLE wc1_seq AS SELECT g AS id, wc1_doc(g) AS doc FROM generate_series(1, 3000) g;
CREATE TABLE wc1_plain AS SELECT * FROM wc1_seq;
CREATE INDEX wc1_plain_doc ON wc1_plain USING bark (doc bark_jsonb_wildcard_ops);
CREATE TABLE wc1_inc AS SELECT * FROM wc1_seq;
CREATE INDEX wc1_inc_doc ON wc1_inc USING bark (doc bark_jsonb_wildcard_ops (include = 'b.c,s,arr'));
CREATE TABLE wc1_tid (id int, doc jsonb);
CREATE INDEX wc1_tid_doc ON wc1_tid USING bark (id, doc bark_jsonb_wildcard_ops);
INSERT INTO wc1_tid SELECT * FROM wc1_seq;
ANALYZE wc1_seq, wc1_plain, wc1_inc, wc1_tid;
CREATE TABLE wc1_quals (n int, q text);
INSERT INTO wc1_quals VALUES
  (1, 'doc #>= ''["s", 10]'''), (2, 'doc #? ''t.u'''), (3, 'doc #< ''["x\\.y", 3]'''),
  (4, 'doc #>= ''["arr", 5]'''), (5, 'doc #>= ''["b.c", 2]'''), (6, 'doc #= ''["s", 1]'''),
  (7, 'doc #= ANY (ARRAY[''["s", 1]'', ''["s", 2]'']::jsonb[])'),
  (8, 'id > 100 AND doc #> ''["s", 3]''');
SELECT n, q, wc_run('wc1_seq', NULL, q, 'seq') AS seq FROM wc1_quals ORDER BY n;
SET client_min_messages = debug1;
SELECT n, t, wc_run(t, i, q, 'index') = wc_run('wc1_seq', NULL, q, 'seq') AS same
  FROM wc1_quals,
       (VALUES (1, 'wc1_plain', 'wc1_plain_doc'), (2, 'wc1_inc', 'wc1_inc_doc'),
               (3, 'wc1_tid', 'wc1_tid_doc')) v(o, t, i)
 WHERE n < 8 OR t = 'wc1_tid'
 ORDER BY n, o;
RESET client_min_messages;
SELECT n, t, wc_run(t, i, q, 'bitmap') = wc_run('wc1_seq', NULL, q, 'seq') AS same
  FROM wc1_quals,
       (VALUES (1, 'wc1_plain', 'wc1_plain_doc'), (2, 'wc1_inc', 'wc1_inc_doc'),
               (3, 'wc1_tid', 'wc1_tid_doc')) v(o, t, i)
 WHERE n < 8 OR t = 'wc1_tid'
 ORDER BY n, o;
-- An ordered scan on the fixed leading column of the compound index.
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_sort = off;
EXPLAIN (COSTS OFF)
SELECT id FROM wc1_tid WHERE doc #>= '["s", 10]' ORDER BY id <~> 1500;
SET client_min_messages = debug1;
SELECT count(*), count(DISTINCT id), md5(string_agg(id::text, ',' ORDER BY id))
  FROM (SELECT id FROM wc1_tid WHERE doc #>= '["s", 10]' ORDER BY id <~> 1500) s;
RESET client_min_messages;
RESET enable_seqscan;
RESET enable_bitmapscan;
RESET enable_sort;
SELECT wc_run('wc1_seq', NULL, 'doc #>= ''["s", 10]''', 'seq');

-- The member "" of the document has the document's own path, so its
-- members repeat the document's paths: {"": {"s": 5}, "s": 6} has two
-- values at s.  It marks [""], above every path.
INSERT INTO wc1_plain VALUES (0, '{"": {"s": 5}, "s": 6}');
INSERT INTO wc1_seq VALUES (0, '{"": {"s": 5}, "s": 6}');
SELECT k FROM bark_wildcard_keys('{"": {"s": 5}, "s": 6}') k ORDER BY k;
SET client_min_messages = debug1;
SELECT wc_run('wc1_plain', 'wc1_plain_doc', 'doc #>= ''["s", 5]''', 'index') =
       wc_run('wc1_seq', NULL, 'doc #>= ''["s", 5]''', 'seq') AS same;
RESET client_min_messages;

-- The flag is decided at every rescan, never at plan time: a generic plan
-- made while q held no array, run again after a document puts one there,
-- uses the set, and so does each rescan of one scan for a path with one.
CREATE TABLE wc_gen (id int, doc jsonb);
CREATE INDEX wc_gen_doc ON wc_gen USING bark (doc bark_jsonb_wildcard_ops);
INSERT INTO wc_gen SELECT g, jsonb_build_object('q', g % 10, 'r', jsonb_build_array(g % 3, g % 3 + 1))
  FROM generate_series(1, 2000) g;
ANALYZE wc_gen;
SET plan_cache_mode = force_generic_plan;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
PREPARE wc_q(jsonb) AS
  SELECT count(*), count(DISTINCT id), md5(string_agg(id::text, ',' ORDER BY id))
    FROM wc_gen WHERE doc #>= $1;
EXPLAIN (COSTS OFF) EXECUTE wc_q('["q", 3]');
SET client_min_messages = debug1;
EXECUTE wc_q('["q", 3]');
RESET client_min_messages;
INSERT INTO wc_gen SELECT g, jsonb_build_object('q', jsonb_build_array(g % 10, g % 10 + 1, g % 10 + 2))
  FROM generate_series(2001, 2100) g;
SET client_min_messages = debug1;
EXECUTE wc_q('["q", 3]');
RESET client_min_messages;
SET enable_seqscan = on;
SET enable_indexscan = off;
EXECUTE wc_q('["q", 3]');
RESET enable_indexscan;
SET enable_seqscan = off;
SET enable_hashjoin = off;
SET enable_mergejoin = off;
SET enable_material = off;
PREPARE wc_q2 AS
  SELECT x, count(*), count(DISTINCT id), md5(string_agg(id::text, ',' ORDER BY id))
    FROM (VALUES ('["r", 1]'::jsonb), ('["q", 7]'), ('["s", 0]')) v(x)
         JOIN wc_gen ON doc #>= x
   GROUP BY x ORDER BY x;
EXPLAIN (COSTS OFF) EXECUTE wc_q2;
SET client_min_messages = debug1;
EXECUTE wc_q2;
RESET client_min_messages;
INSERT INTO wc_gen VALUES (3000, '{"s": [0, 1]}');
SET client_min_messages = debug1;
EXECUTE wc_q2;
RESET client_min_messages;
SET enable_seqscan = on;
SET enable_indexscan = off;
EXECUTE wc_q2;
DEALLOCATE wc_q;
DEALLOCATE wc_q2;
RESET plan_cache_mode;
RESET enable_seqscan;
RESET enable_bitmapscan;
RESET enable_indexscan;
RESET enable_hashjoin;
RESET enable_mergejoin;
RESET enable_material;
SELECT bark_index_check('wc_gen_doc'), bark_index_check('wc1_plain_doc'),
       bark_index_check('wc1_inc_doc'), bark_index_check('wc1_tid_doc');

DROP TABLE wc1_seq, wc1_plain, wc1_inc, wc1_tid, wc1_quals, wc_gen;
DROP FUNCTION wc1_doc;
-- Sorting by a path: doc |<| 'a' is the smallest key [a, value] (MongoDB's
-- array sort rule, across types in jsonb_cmp order), NULL without one.
-- The index serves it without a Sort: the path's range, then the rest of
-- the column (orderrest) for the documents without the path, which come
-- back last.  Every result is compared, row by row, with the key that
-- bark_wildcard_keys gives under a sequential scan: each row once, the
-- order value exact, nondecreasing, NULLs last; with a filter (rechecked,
-- not positioning), a LIMIT, a NULLS FIRST index and nested-loop rescans.
CREATE TABLE wo (id int, doc jsonb);
INSERT INTO wo SELECT g,
  CASE g % 9
    WHEN 0 THEN jsonb_build_object('a', g % 37)
    WHEN 1 THEN jsonb_build_object('a', jsonb_build_array(g % 41, (g * 7) % 41, 's' || g % 5))
    WHEN 2 THEN jsonb_build_object('b', g)
    WHEN 3 THEN jsonb_build_object('a', 'str' || g % 13)
    WHEN 4 THEN jsonb_build_object('a', (g % 2 = 0))
    WHEN 5 THEN jsonb_build_object('a', jsonb_build_array(jsonb_build_array(g % 3, 1), g % 50))
    WHEN 6 THEN jsonb_build_object('a', NULL::int)
    WHEN 7 THEN jsonb_build_object('a', '[]'::jsonb, 'c', g)
    ELSE CASE WHEN g % 2 = 0 THEN NULL ELSE to_jsonb(g) END END
  FROM generate_series(1, 3000) g;
CREATE INDEX wo_doc ON wo USING bark (doc bark_jsonb_wildcard_ops);
ANALYZE wo;
SELECT bark_wildcard_boundaries('a'::text, 7::int2);
CREATE TABLE wo_want AS SELECT id, doc,
  (SELECT k FROM bark_wildcard_keys(doc) k
    WHERE jsonb_array_length(k) = 2 AND k->>0 = 'a' ORDER BY k LIMIT 1) AS k
  FROM wo;
SELECT count(*) FILTER (WHERE k IS NULL) AS without_a, count(*) FROM wo_want;
-- wo_check(q): q's rows (id, k) in its order, compared with wo_want.
CREATE FUNCTION wo_check(q text, OUT n bigint, OUT ids bigint,
                         OUT wrong_value bigint, OUT out_of_order bigint,
                         OUT nulls bigint)
LANGUAGE plpgsql AS $$
BEGIN
  EXECUTE format($f$
    WITH got AS (SELECT row_number() OVER () AS rn, id, k FROM (%s) s),
         lagged AS (SELECT rn, k, lag(k) OVER (ORDER BY rn) AS pk FROM got)
    SELECT (SELECT count(*) FROM got), (SELECT count(DISTINCT id) FROM got),
           (SELECT count(*) FROM got JOIN wo_want w USING (id)
             WHERE got.k IS DISTINCT FROM w.k),
           (SELECT count(*) FROM lagged
             WHERE (pk IS NULL AND k IS NOT NULL AND rn > 1) OR k < pk),
           (SELECT count(*) FROM got WHERE k IS NULL)$f$, q)
    INTO n, ids, wrong_value, out_of_order, nulls;
END $$;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_sort = off;
EXPLAIN (COSTS OFF) SELECT id FROM wo ORDER BY doc |<| 'a';
SELECT * FROM wo_check('SELECT id, doc |<| ''a'' AS k FROM wo ORDER BY doc |<| ''a''');
EXPLAIN (COSTS OFF)
SELECT id FROM wo WHERE doc #? 'b' OR doc #= '["a", 3]' ORDER BY doc |<| 'a';
SELECT * FROM wo_check('SELECT id, doc |<| ''a'' AS k FROM wo
                        WHERE doc #? ''b'' OR doc #= ''["a", 3]'' ORDER BY doc |<| ''a''');
SELECT count(*) FROM wo_want WHERE doc #? 'b' OR doc #= '["a", 3]';
SELECT * FROM wo_check('SELECT id, doc |<| ''a'' AS k FROM wo ORDER BY doc |<| ''a'' LIMIT 500');
SELECT (SELECT array_agg(k) FROM (SELECT doc |<| 'a' AS k FROM wo ORDER BY doc |<| 'a' LIMIT 500) s) =
       (SELECT array_agg(k) FROM (SELECT k FROM wo_want ORDER BY k NULLS LAST LIMIT 500) x)
       AS limit_is_the_500_smallest;
SET enable_hashjoin = off;
SET enable_mergejoin = off;
SET enable_material = off;
SELECT p, count(*), count(DISTINCT id)
  FROM (VALUES ('a'), ('b'), ('zz')) v(p),
       LATERAL (SELECT id FROM wo ORDER BY doc |<| p LIMIT 2900) s
 GROUP BY p ORDER BY p;
RESET enable_hashjoin;
RESET enable_mergejoin;
RESET enable_material;
CREATE INDEX wo_doc_nf ON wo USING bark (doc bark_jsonb_wildcard_ops NULLS FIRST);
DROP INDEX wo_doc;
SELECT * FROM wo_check('SELECT id, doc |<| ''a'' AS k FROM wo ORDER BY doc |<| ''a''');
RESET enable_seqscan;
RESET enable_bitmapscan;
RESET enable_sort;
SELECT bark_index_check('wo_doc_nf');
-- A path the projection drops has no keys to sort by.
CREATE INDEX wo_inc ON wo USING bark (doc bark_jsonb_wildcard_ops (include = 'b'));
DROP INDEX wo_doc_nf;
SET enable_seqscan = off;
SET enable_sort = off;
SELECT id FROM wo ORDER BY doc |<| 'a' LIMIT 1;
RESET enable_seqscan;
RESET enable_sort;
DROP TABLE wo, wo_want;
DROP FUNCTION wo_check;

-- Numbers are written in one form, their scale trimmed, so keys that
-- compare equal have equal bytes and coalesce (the class's equalimage):
-- 1, 1.0 and 1.00 at one path are one key, inside an inner array too, and
-- |<| reports that key.  wc_num's index takes its rows by insert; wc_numb's
-- is built over them, with prefix compression.  Every query agrees with a
-- sequential scan, each index holds exactly the heap's keys (compared as
-- text, so as bytes), and their equal keys share LIST or POSTING entries.
CREATE TABLE wc_num (id int, doc jsonb);
CREATE INDEX wc_num_doc ON wc_num USING bark (doc bark_jsonb_wildcard_ops);
INSERT INTO wc_num SELECT g, jsonb_build_object('a',
    (ARRAY['1', '1.0', '1.00', '2.50', '2.5', '[1.0, 1, 3]',
           '[[1.0], [1], [1.00, 0.0]]'])[1 + g % 7]::jsonb)
  FROM generate_series(1, 1400) g;
CREATE TABLE wc_numb AS SELECT * FROM wc_num;
CREATE INDEX wc_numb_doc ON wc_numb USING bark (doc bark_jsonb_wildcard_ops)
  WITH (prefix_compression = on);
SELECT DISTINCT k::text FROM wc_num, bark_wildcard_keys(doc) k ORDER BY 1;
SELECT DISTINCT (doc |<| 'a')::text AS least FROM wc_num ORDER BY 1;
SELECT q, wc_run('wc_num', NULL, q, 'seq') AS seq,
       wc_run('wc_num', 'wc_num_doc', q, 'index') = wc_run('wc_num', NULL, q, 'seq') AND
       wc_run('wc_num', 'wc_num_doc', q, 'bitmap') = wc_run('wc_num', NULL, q, 'seq') AND
       wc_run('wc_numb', 'wc_numb_doc', q, 'index') = wc_run('wc_num', NULL, q, 'seq') AND
       wc_run('wc_numb', 'wc_numb_doc', q, 'bitmap') = wc_run('wc_num', NULL, q, 'seq') AS same
  FROM unnest(ARRAY['doc #= ''["a", 1]''', 'doc #= ''["a", 1.00]''', 'doc #< ''["a", 2.500]''',
                    'doc #>= ''["a", 2.5]''', 'doc #= ''["a", [1]]''',
                    'doc #= ''["a", [1.0, 0]]''', 'doc #? ''a''']) q;
SELECT i, (SELECT count(*) FROM (SELECT key::text, tid, marker FROM wc_index_keys(i::regclass)
                                 EXCEPT ALL
                                 SELECT key::text, tid, marker FROM wc_heap_keys(t::regclass, NULL, NULL)) x) AS index_only,
       (SELECT count(*) FROM (SELECT key::text, tid, marker FROM wc_heap_keys(t::regclass, NULL, NULL)
                              EXCEPT ALL
                              SELECT key::text, tid, marker FROM wc_index_keys(i::regclass)) x) AS heap_only
  FROM (VALUES ('wc_num_doc', 'wc_num'), ('wc_numb_doc', 'wc_numb')) v(i, t) ORDER BY i;
SELECT i, shape, count(*) AS entries, sum(ntids) AS tids
  FROM unnest(ARRAY['wc_num_doc', 'wc_numb_doc']) i, bark_page_items(i, 1)
 GROUP BY i, shape ORDER BY i, shape;
-- An ordered scan reads each row once, nondecreasing, with |<|'s value.
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_sort = off;
EXPLAIN (COSTS OFF) SELECT id FROM wc_num ORDER BY doc |<| 'a';
SELECT count(*), count(DISTINCT id), count(*) FILTER (WHERE k < pk) AS out_of_order,
       count(*) FILTER (WHERE k::text IS DISTINCT FROM want::text) AS wrong_value
  FROM (SELECT id, k, lag(k) OVER (ORDER BY rn) AS pk,
               (SELECT kk FROM wc_num w, bark_wildcard_keys(w.doc) kk
                 WHERE w.id = s.id AND jsonb_array_length(kk) = 2 ORDER BY kk LIMIT 1) AS want
          FROM (SELECT row_number() OVER () AS rn, id, k
                  FROM (SELECT id, doc |<| 'a' AS k FROM wc_num ORDER BY doc |<| 'a') o) s) x;
RESET enable_seqscan;
RESET enable_bitmapscan;
RESET enable_sort;
SELECT bark_index_check('wc_num_doc'), bark_index_check('wc_numb_doc');
-- WSEL: the wildcard operators estimate without scanning the stats sample, so
-- their row estimate does not change with default_statistics_target, and the
-- plan the planner picks is the same shape as before (index/bitmap for the
-- selective operators).  (COSTS OFF hides the row count; we read it from the
-- planner directly and check it is identical at target 100 and 1000.)
ANALYZE wc_num;
SET enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT id FROM wc_num WHERE doc #= '["a", 1]';
EXPLAIN (COSTS OFF) SELECT id FROM wc_num WHERE doc #< '["a", 5]';
EXPLAIN (COSTS OFF) SELECT id FROM wc_num WHERE doc #? 'a';
RESET enable_seqscan;
CREATE FUNCTION wc_rows(q text) RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE p jsonb; r bigint;
BEGIN
  EXECUTE 'EXPLAIN (FORMAT json) SELECT id FROM wc_num WHERE ' || q INTO p;
  r := (p -> 0 -> 'Plan' ->> 'Plan Rows')::bigint;
  RETURN r;
END$$;
SET default_statistics_target = 100;
ANALYZE wc_num;
SELECT wc_rows('doc #= ''["a", 1]''') AS eq100,
       wc_rows('doc #< ''["a", 5]''') AS lt100,
       wc_rows('doc #? ''a''') AS ex100 INTO TEMP wc_est100;
SET default_statistics_target = 1000;
ANALYZE wc_num;
-- Identical estimates prove the estimator never touched the (now 10x larger)
-- sample; a sample-scanning estimator would move with the target.
SELECT wc_rows('doc #= ''["a", 1]''') = eq100 AS eq_stable,
       wc_rows('doc #< ''["a", 5]''') = lt100 AS lt_stable,
       wc_rows('doc #? ''a''') = ex100 AS ex_stable
  FROM wc_est100;
RESET default_statistics_target;
DROP FUNCTION wc_rows;
DROP TABLE wc_num, wc_numb;

-- The level-1 entry cache shares its chunk with the markers this backend
-- has seen, after them.  A new marker grows the chunk and moves the table
-- up past it; the table survives.  Wide keys give a tree with many level-1
-- pages; lookups warm the table, then each insert brings a path with a new
-- array (a new marker), and every lookup after it must still match a
-- sequential scan, by an index scan that starts below the root.
CREATE TABLE wc_l1 (id int, doc jsonb);
INSERT INTO wc_l1 SELECT g, jsonb_build_object('k', lpad(g::text, 6, '0') || repeat('x', 300))
  FROM generate_series(1, 6000) g;
CREATE INDEX wc_l1_doc ON wc_l1 USING bark (doc bark_jsonb_wildcard_ops);
SET enable_seqscan = off; SET enable_bitmapscan = off;
SELECT count(*) FROM generate_series(1, 6000, 9) g, LATERAL
  (SELECT id FROM wc_l1 WHERE doc #= jsonb_build_array('k', lpad(g::text, 6, '0') || repeat('x', 300))) s;
RESET enable_seqscan; RESET enable_bitmapscan;
DO $$
BEGIN
  FOR i IN 1..12 LOOP
    INSERT INTO wc_l1 VALUES (10000 + i, jsonb_build_object('arr' || i, jsonb_build_array(i, i + 1)));
  END LOOP;
END $$;
SELECT count(*) FILTER (WHERE seq <> idx) AS mismatches, count(*) AS checks
  FROM (SELECT wc_run('wc_l1', NULL, q, 'seq') AS seq, wc_run('wc_l1', 'wc_l1_doc', q, 'index') AS idx
          FROM (SELECT format('doc #= %L', jsonb_build_array('k', lpad(g::text, 6, '0') || repeat('x', 300))) AS q
                  FROM generate_series(1, 6000, 37) g
                UNION ALL
                SELECT format('doc #? %L', 'arr' || i) FROM generate_series(1, 12) i) qs) r;
DROP TABLE wc_l1;

DROP TABLE wc_seq, wc_plain, wc_inc, wc_exc, wc_tid, wc_quals;
DROP FUNCTION wc_run, wc_compare, wc_jpath, wc_doc, wc_val, wc_heap_keys, wc_index_keys;
DROP EXTENSION bark_wildcard;
