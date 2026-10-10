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
