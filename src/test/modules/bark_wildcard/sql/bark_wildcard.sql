-- A wildcard operator class over jsonb, as MongoDB's wildcard indexes.
CREATE EXTENSION bark_wildcard;
CREATE EXTENSION amcheck;
SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'bark_jsonb_wildcard_ops';

-- The keys procedure 7 should produce, computed independently in SQL: one
-- [path, scalar] per scalar, [path, {}] / [path, []] for an empty container,
-- an array's elements at the array's own path, a nested array as a value,
-- and a marker [path] for every path that holds an array.  Member names
-- have dots and backslashes escaped.
CREATE FUNCTION wc_expected(doc jsonb, OUT k jsonb) RETURNS SETOF jsonb
LANGUAGE sql IMMUTABLE AS $$
WITH RECURSIVE w(path, v, inarray) AS (
  SELECT ''::text, doc, false
  UNION ALL
  SELECT CASE WHEN x.member IS NULL THEN w.path
              WHEN w.path = '' THEN x.member
              ELSE w.path || '.' || x.member END,
         x.value, x.member IS NULL
  FROM w, LATERAL (
    SELECT replace(replace(key, '\', '\\'), '.', '\.') AS member, value
      FROM jsonb_each(CASE WHEN jsonb_typeof(w.v) = 'object' THEN w.v END)
    UNION ALL
    SELECT NULL, value
      FROM jsonb_array_elements(CASE WHEN jsonb_typeof(w.v) = 'array'
                                      AND NOT w.inarray THEN w.v END)) x
)
SELECT jsonb_build_array(path, v) FROM w
 WHERE jsonb_typeof(v) NOT IN ('object', 'array')
    OR (jsonb_typeof(v) = 'array' AND inarray)
    OR v = '{}' OR (v = '[]' AND NOT inarray)
UNION ALL
SELECT jsonb_build_array(path) FROM w
 WHERE jsonb_typeof(v) = 'array' AND NOT inarray
$$;

CREATE TABLE wc_docs (id int, doc jsonb);
INSERT INTO wc_docs VALUES
  (1, '{"a": 1}'),
  (2, '{"a": {"b": 2, "c": "x"}, "d": null}'),
  (3, '{"a": [1, 2, 2, {"b": 3}], "e": []}'),
  (4, '{"a": [[1, 2], [3]], "f": {}}'),
  (5, '{"a.b": 1, "x\\y": 2, "n": {"m": [true, false, null]}}'),
  (6, '[1, {"a": 2}, [3]]'),
  (7, '42'),
  (8, '"s"'),
  (9, '{}'),
  (10, '[]'),
  (11, '{"a": 1.0, "b": 1.00, "c": -0, "d": 1e3}');

-- Procedure 7's keys and markers equal the independent computation, as
-- multisets, for every document.
SELECT id,
       (SELECT array_agg(k ORDER BY k) FROM bark_wildcard_keys(doc) k) IS NOT DISTINCT FROM
       (SELECT array_agg(k ORDER BY k) FROM wc_expected(doc) k) AS same,
       (SELECT count(*) FROM bark_wildcard_keys(doc)) AS nkeys
FROM wc_docs ORDER BY id;
SELECT bark_wildcard_keys(doc) FROM wc_docs WHERE id = 3;

-- Projections: include keeps those paths and those below them; exclude the
-- others; both at once is an error.
SELECT k FROM bark_wildcard_keys('{"a": {"b": 1, "c": 2}, "ab": 3, "d": [4]}', 'a.b,d') k ORDER BY k;
SELECT k FROM bark_wildcard_keys('{"a": {"b": 1, "c": 2}, "ab": 3, "d": [4]}', NULL, 'a') k ORDER BY k;
SELECT bark_wildcard_keys('{}', 'a', 'b');
SELECT bark_wildcard_keys('{}', 'a,,b');

-- Procedure 8: the boundaries of each operator for a value of each type, at
-- one path.  Each type is a bracket of the path's range, in jsonb_cmp's
-- order null < string < number < boolean < array < object: a string ends
-- below [p, -Infinity] (a search key only: jsonb stores no infinite
-- number), a number below [p, false], an array below [p, {}], and [p, {}]
-- is the path's last key (the only object the class indexes).  #? is the
-- whole path, [p, null] to [p, {}].
SELECT t, s, bark_wildcard_boundaries(jsonb_build_array('a', val), s::int2)
  FROM (VALUES (1, 'null', 'null'::jsonb), (2, 'string', '"s"'), (3, 'number', '5'),
               (4, 'boolean', 'true'), (5, 'array', '[1, 2]'), (6, 'object', '{}')) x(o, t, val),
       generate_series(1, 5) s
 ORDER BY o, s;
SELECT bark_wildcard_boundaries('a'::text, 6::int2);
-- A path the projection drops has no keys: the scan reads every key and
-- the NULL entries, and rechecks (procedure 9 says maybe).
SELECT bark_wildcard_boundaries('["b", 1]'::jsonb, 3::int2, 'a'),
       bark_wildcard_boundaries('a.b'::text, 6::int2, NULL, 'a');

-- The boundaries against real keys in jsonb_cmp's order: the keys of one
-- document holding a value of each type at path a (and keys of the
-- neighbouring paths A, a.b and a0), sorted with jsonb's order.  For each
-- key: the types whose bracket at a holds it (the union of #<= and #>= for
-- a value of that type), whether #? a holds it, and which of the five
-- operators against ["a", 0] hold it.  Each key of a is in its own type's
-- bracket only; keys of other paths and the marker ["a"] are in none, and
-- outside #?.
SELECT k,
       (SELECT string_agg(t, ',' ORDER BY o)
          FROM (VALUES (1, 'null', 'null'::jsonb), (2, 'string', '"x"'), (3, 'number', '0'),
                       (4, 'boolean', 'true'), (5, 'array', '[0]'), (6, 'object', '{}')) b(o, t, v)
         WHERE bark_wildcard_in_boundaries(k, jsonb_build_array('a', v), 2::int2)
            OR bark_wildcard_in_boundaries(k, jsonb_build_array('a', v), 4::int2)) AS brackets,
       bark_wildcard_in_boundaries(k, 'a'::text, 6::int2) AS exists_a,
       (SELECT string_agg(op, ' ' ORDER BY s)
          FROM (VALUES (1, '<'), (2, '<='), (3, '='), (4, '>='), (5, '>')) o(s, op)
         WHERE bark_wildcard_in_boundaries(k, '["a", 0]'::jsonb, s::int2)) AS vs_0
  FROM bark_wildcard_keys('{"A": 1, "a": [null, "", "s", -1e20, -5, 0, 2.5, 1e20, false, true,
                                         [], [1, 2], [1, 2, 3], {}, {"b": 1}], "a0": 1}') k
 ORDER BY k;

-- An index over the documents, built and inserted into, with and without a
-- projection, and a compound (tenant, doc) index.
CREATE INDEX wc_docs_doc ON wc_docs USING bark (doc bark_jsonb_wildcard_ops);
CREATE INDEX wc_docs_inc ON wc_docs USING bark (doc bark_jsonb_wildcard_ops (include = 'a'));
CREATE INDEX wc_docs_both ON wc_docs USING bark (doc bark_jsonb_wildcard_ops (include = 'a', exclude = 'b'));
CREATE INDEX wc_docs_tenant ON wc_docs USING bark (id, doc bark_jsonb_wildcard_ops);
INSERT INTO wc_docs SELECT g, jsonb_build_object('a', g % 7, 'b', jsonb_build_array(g, g + 1),
                                                 'c', jsonb_build_object('d', 'v' || g % 3))
  FROM generate_series(100, 2100) g;
SELECT bark_index_check('wc_docs_doc'), bark_index_check('wc_docs_inc'),
       bark_index_check('wc_docs_tenant');
-- The index holds one member per distinct data key of every document (BARK
-- removes a row's repeated keys), and one NULL key for a document with none
-- (here, one whose paths the projection drops): its meta page's key count
-- (members, markers excluded) after VACUUM equals the independent
-- computation's, with and without the projection.  (amcheck's
-- heapallindexed does not check multikey indexes yet.)
CREATE EXTENSION pageinspect;
VACUUM wc_docs;
SELECT (SELECT nkeys FROM bark_metap('wc_docs_doc')) =
       (SELECT sum(greatest(1, (SELECT count(DISTINCT k) FROM wc_expected(doc) k
                                 WHERE jsonb_array_length(k) = 2))) FROM wc_docs) AS all_keys,
       (SELECT nkeys FROM bark_metap('wc_docs_inc')) =
       (SELECT sum(greatest(1, (SELECT count(DISTINCT k) FROM wc_expected(doc) k
                                 WHERE jsonb_array_length(k) = 2
                                   AND (k->>0 = 'a' OR k->>0 LIKE 'a.%')))) FROM wc_docs) AS included_keys,
       (SELECT flags FROM bark_metap('wc_docs_doc')) & 1 = 1 AS multikey;
DROP TABLE wc_docs;
DROP FUNCTION wc_expected(jsonb);

-- The operators find a path's values by descending to it; they must agree
-- with procedure 7's keys of the whole document, which is what the index
-- holds: doc OP q is true exactly when some key [path, v] of doc, v in q's
-- type bracket, compares so with q in jsonb's order; #? is true when the
-- path has a key; |<| is the least key at the path.
CREATE TABLE wc_opdocs (id int, doc jsonb);
INSERT INTO wc_opdocs VALUES
  (1, '{"a": 1}'), (2, '{"a": {"b": 2, "c": "x"}, "d": null}'),
  (3, '{"a": [1, 2, 2, {"b": 3}], "e": []}'), (4, '{"a": [[1, 2], [3]], "f": {}}'),
  (5, '{"a.b": 1, "x\\y": 2, "n": {"m": [true, false, null]}}'),
  (6, '[1, {"a": 2}, [3]]'), (7, '42'), (8, '"s"'), (9, '{}'), (10, '[]'),
  (11, '{"a": 1.0, "b": 1.00, "c": -0, "d": 1e3}'),
  (12, '{"": {"a": 1}, "a": 2}'), (13, '[{"": [{"a": 3}, "y"]}, {"a": {}}]'),
  (14, '{"a": {"": {"b": [5, [6], {}]}}, "": 7}'), (15, '{"": {"": 8}}'),
  (16, '{"a": [[{"b": 1}]], "b": [{"c": [1, "1", true, null, [], {}]}]}'),
  (17, '{"a\\.b": {"c\\\\d": "e"}, "a": {"b": 9}}');
CREATE TABLE wc_opq AS
  SELECT id, doc, p, q, s
    FROM wc_opdocs,
         LATERAL (SELECT k->>0 AS p FROM bark_wildcard_keys(doc) k
                  UNION SELECT unnest(ARRAY['', 'zz', 'a.', 'a\q', '.a', 'a.b.c'])) ps,
         LATERAL (SELECT jsonb_build_array(p, v) AS q
                    FROM (SELECT k->1 AS v FROM bark_wildcard_keys(doc) k
                           WHERE k->>0 = p AND jsonb_array_length(k) = 2
                          UNION SELECT unnest('{null, "\"\"", "\"y\"", 0, 1, 1.5, -1, true, false, "[]", "[1, 2]", "{}"}'::jsonb[])) vs) qs,
         generate_series(1, 5) s;
SELECT count(*) AS cases, count(*) FILTER (WHERE got IS DISTINCT FROM want) AS wrong,
       count(*) FILTER (WHERE want) AS true_cases
  FROM (SELECT CASE s WHEN 1 THEN doc #< q WHEN 2 THEN doc #<= q WHEN 3 THEN doc #= q
                      WHEN 4 THEN doc #>= q ELSE doc #> q END AS got,
               EXISTS (SELECT FROM bark_wildcard_keys(doc) k
                        WHERE jsonb_array_length(k) = 2 AND k->>0 = q->>0
                          AND jsonb_typeof(k->1) = jsonb_typeof(q->1)
                          AND CASE s WHEN 1 THEN k < q WHEN 2 THEN k <= q WHEN 3 THEN k = q
                                     WHEN 4 THEN k >= q ELSE k > q END) AS want
          FROM wc_opq) x;
SELECT count(*) FILTER (WHERE (doc #? p) IS DISTINCT FROM
                              EXISTS (SELECT FROM bark_wildcard_keys(doc) k
                                       WHERE jsonb_array_length(k) = 2 AND k->>0 = p)) AS exists_wrong,
       count(*) FILTER (WHERE (doc |<| p) IS DISTINCT FROM
                              (SELECT k FROM bark_wildcard_keys(doc) k
                                WHERE jsonb_array_length(k) = 2 AND k->>0 = p
                                ORDER BY k LIMIT 1)) AS least_wrong,
       count(*) FILTER (WHERE doc #? p) AS exists_true
  FROM (SELECT DISTINCT id, doc, p FROM wc_opq) x;
DROP TABLE wc_opq, wc_opdocs;

-- HOT updates (barkcomparedatums).  Under include = 'a', a change to
-- another path leaves the keys as they are: HOT.  A change at a: not HOT.
-- 4 to 4.000 is HOT, since procedure 7 trims a number's scale, so both
-- give the same key bytes.  A scalar becoming a one-element array gives
-- the same [a, value] key but a new marker [a], an insert the index
-- needs: not HOT, and the marker is then in the index; a second row doing
-- the same afterwards is not HOT either, since its old value lacks the
-- marker.  The array going back to a scalar drops the marker: HOT.  The
-- index without a projection sees every change.
CREATE TABLE wc_hot (id int, doc jsonb) WITH (fillfactor = 10);
CREATE INDEX wc_hot_a ON wc_hot USING bark (doc bark_jsonb_wildcard_ops (include = 'a'));
CREATE INDEX wc_hot_ia ON wc_hot USING bark (id, doc bark_jsonb_wildcard_ops (include = 'a'));
INSERT INTO wc_hot SELECT g, jsonb_build_object('a', g % 5, 'b', g) FROM generate_series(1, 20) g;
CREATE FUNCTION wc_hot_count(OUT upd bigint, OUT hot bigint) LANGUAGE sql AS $$
  SELECT pg_stat_get_xact_tuples_updated('wc_hot'::regclass),
         pg_stat_get_xact_tuples_hot_updated('wc_hot'::regclass) $$;
BEGIN;
UPDATE wc_hot SET doc = jsonb_set(doc, '{b}', to_jsonb(id * 10));
SELECT 'path not indexed' AS step, * FROM wc_hot_count();
UPDATE wc_hot SET doc = doc || '{"c": {"d": [1, 2]}}' WHERE id <= 5;
SELECT 'array at a path not indexed' AS step, * FROM wc_hot_count();
UPDATE wc_hot SET doc = jsonb_set(doc, '{a}', '7') WHERE id = 1;
SELECT 'indexed path' AS step, * FROM wc_hot_count();
UPDATE wc_hot SET doc = jsonb_set(doc, '{a}', '4.000') WHERE id = 4;
SELECT 'indexed path, 4 to 4.000' AS step, * FROM wc_hot_count();
UPDATE wc_hot SET doc = jsonb_set(doc, '{a}', jsonb_build_array(doc->'a')) WHERE id = 2;
SELECT 'scalar to one-element array' AS step, * FROM wc_hot_count();
UPDATE wc_hot SET doc = jsonb_set(doc, '{a}', jsonb_build_array(doc->'a')) WHERE id = 3;
SELECT 'again, marker in the index' AS step, * FROM wc_hot_count();
UPDATE wc_hot SET doc = jsonb_set(doc, '{a}', doc->'a'->0) WHERE id = 2;
SELECT 'array back to scalar' AS step, * FROM wc_hot_count();
COMMIT;
SELECT key FROM bark_wildcard_entries('wc_hot_a') WHERE marker;
SELECT bark_index_check('wc_hot_a', true), bark_index_check('wc_hot_ia', true);
-- The counts start again from zero once this backend's are flushed.
SELECT pg_stat_force_next_flush();
CREATE INDEX wc_hot_all ON wc_hot USING bark (doc bark_jsonb_wildcard_ops);
BEGIN;
UPDATE wc_hot SET doc = jsonb_set(doc, '{b}', to_jsonb(id)) WHERE id <= 4;
SELECT 'no projection' AS step, * FROM wc_hot_count();
COMMIT;
SELECT bark_index_check('wc_hot_all', true);
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT array_agg(id ORDER BY id) FROM wc_hot WHERE doc #= '["a", 4]';
SELECT array_agg(id ORDER BY id) FROM wc_hot WHERE doc #= '["a", 2]';
SELECT array_agg(id ORDER BY id) FROM wc_hot WHERE doc #= '["b", 30]';
SET enable_indexscan = off;
SET enable_bitmapscan = on;
SELECT array_agg(id ORDER BY id) FROM wc_hot WHERE doc #= '["a", 4]';
SELECT array_agg(id ORDER BY id) FROM wc_hot WHERE doc #= '["a", 2]';
RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_bitmapscan;
SELECT array_agg(id ORDER BY id) FROM wc_hot WHERE doc->'a' @> '4' OR doc->'a' = '4';
SELECT array_agg(id ORDER BY id) FROM wc_hot WHERE doc->'a' @> '2' OR doc->'a' = '2';
DROP TABLE wc_hot;
DROP FUNCTION wc_hot_count;
