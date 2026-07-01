CREATE EXTENSION pg_fts VERSION '1.0';

-- ftsdoc: analysis, output shows terms with term frequencies
SELECT to_ftsdoc('The quick brown fox, the QUICK fox!');
SELECT 'the quick brown fox'::ftsdoc;
SELECT to_ftsdoc('');                       -- empty doc
SELECT ftsdoc_length(to_ftsdoc('a b c a b a'));  -- doclen counts tokens

-- ftsquery: parsing and canonical output
SELECT 'quick & brown'::ftsquery;
SELECT 'quick | brown'::ftsquery;
SELECT '!slow'::ftsquery;
SELECT 'quick brown fox'::ftsquery;          -- implicit AND
SELECT to_ftsquery('(quick OR slow) AND fox');
SELECT to_ftsquery('quick and not slow');    -- keyword operators
SELECT 'QUICK'::ftsquery;                     -- folding

-- syntax errors
SELECT 'quick &'::ftsquery;                   -- dangling operator
SELECT '(quick'::ftsquery;                     -- unbalanced paren

-- @@@ match operator
SELECT to_ftsdoc('the quick brown fox') @@@ 'quick'::ftsquery;         -- t
SELECT to_ftsdoc('the quick brown fox') @@@ 'slow'::ftsquery;          -- f
SELECT to_ftsdoc('the quick brown fox') @@@ 'quick & fox'::ftsquery;   -- t
SELECT to_ftsdoc('the quick brown fox') @@@ 'quick & slow'::ftsquery;  -- f
SELECT to_ftsdoc('the quick brown fox') @@@ 'quick | slow'::ftsquery;  -- t
SELECT to_ftsdoc('the quick brown fox') @@@ '!slow'::ftsquery;         -- t
SELECT to_ftsdoc('the quick brown fox') @@@ '!fox'::ftsquery;          -- f
SELECT to_ftsdoc('the quick brown fox') @@@ 'quick & !slow'::ftsquery; -- t

-- commutator form
SELECT 'quick'::ftsquery @@@ to_ftsdoc('the quick brown fox');         -- t

-- empty query matches nothing
SELECT to_ftsdoc('anything') @@@ ''::ftsquery;                         -- f

-- end-to-end: WHERE on a table, sequential scan
CREATE TABLE docs (id int, body text);
INSERT INTO docs VALUES
  (1, 'the quick brown fox'),
  (2, 'a slow green turtle'),
  (3, 'quick turtles are rare'),
  (4, 'brown bears and quick foxes');

SELECT id FROM docs
WHERE to_ftsdoc(body) @@@ 'quick & !turtle'::ftsquery
ORDER BY id;

SELECT id FROM docs
WHERE to_ftsdoc(body) @@@ '(quick | slow) & !fox'::ftsquery
ORDER BY id;

-- binary send/recv round-trip is exercised by COPY BINARY in the framework;
-- here just confirm send produces bytea without error.
SELECT octet_length(ftsdoc_send(to_ftsdoc('round trip test'))) > 0 AS ftsdoc_send_ok;
SELECT octet_length(ftsquery_send('a & (b | !c)'::ftsquery)) > 0 AS ftsquery_send_ok;

-- adversarial / edge cases: must not crash; must parse or error cleanly
SELECT to_ftsquery(repeat('(', 100) || 'a' || repeat(')', 100)) IS NOT NULL AS deep_nesting_ok;
SELECT '!!!!a'::ftsquery;                -- stacked NOT
SELECT '(((a)))'::ftsquery;              -- redundant parens
SELECT to_ftsquery('   ')::text AS whitespace_only;   -- empty query
SELECT to_ftsquery('a & & b');           -- double operator -> error
SELECT to_ftsquery('a | b & c');         -- precedence: & binds tighter than |
SELECT ftsdoc_length(to_ftsdoc(repeat('word ', 1000))) AS many_repeats_len;

DROP TABLE docs;

-- Stage 2: analyzer reusing an installed text search configuration.
ALTER EXTENSION pg_fts UPDATE TO '1.1';

-- english config stems and drops stopwords: 'running the races' -> run, race
SELECT to_ftsdoc('english'::regconfig, 'running the races quickly');
-- stopwords ('the','a','of') are removed by the english dictionary
SELECT to_ftsdoc('english'::regconfig, 'the cat and a dog');
-- doclen counts positions produced by the parser (stopwords still counted)
SELECT ftsdoc_length(to_ftsdoc('english'::regconfig, 'the quick brown fox'));
-- stemming makes a query match across inflections
SELECT to_ftsdoc('english'::regconfig, 'the foxes were running')
       @@@ 'fox & run'::ftsquery AS stemmed_match;

-- Stage 4: BM25 scoring.
ALTER EXTENSION pg_fts UPDATE TO '1.2';

-- score is positive when a query term is present, zero when absent
SELECT round(fts_bm25(to_ftsdoc('quick brown fox'), 'fox'::ftsquery,
                      1000, 4.0)::numeric, 4) AS present_gt_0;
SELECT fts_bm25(to_ftsdoc('quick brown fox'), 'turtle'::ftsquery,
                1000, 4.0) AS absent_is_0;

-- length normalization: same tf, longer doc scores lower
SELECT fts_bm25(to_ftsdoc('fox'), 'fox'::ftsquery, 1000, 10.0)
     > fts_bm25(to_ftsdoc('fox ' || repeat('pad ', 40)), 'fox'::ftsquery, 1000, 10.0)
       AS shorter_scores_higher;

-- IDF: a rarer term (low df) contributes more than a common one (high df)
SELECT fts_bm25(to_ftsdoc('rare common'), 'rare'::ftsquery, 1000, 2.0, ARRAY[2.0])
     > fts_bm25(to_ftsdoc('rare common'), 'common'::ftsquery, 1000, 2.0, ARRAY[900.0])
       AS rare_scores_higher;

-- higher term frequency scores higher (saturating)
SELECT fts_bm25(to_ftsdoc('fox fox fox'), 'fox'::ftsquery, 1000, 3.0)
     > fts_bm25(to_ftsdoc('fox pad pad'), 'fox'::ftsquery, 1000, 3.0)
       AS more_tf_scores_higher;

-- Stage 9: BM25 variants.
ALTER EXTENSION pg_fts UPDATE TO '1.4';
-- all variants score presence > absence
SELECT variant,
       fts_bm25_opts(to_ftsdoc('quick fox'), 'fox'::ftsquery,
                     1000, 3.0, 1.2, 0.75, variant, ARRAY[10.0]) > 0 AS positive
FROM unnest(ARRAY['lucene','robertson','atire','bm25+']) AS variant
ORDER BY variant;
-- bm25+ >= lucene for the same inputs (delta floor)
SELECT fts_bm25_opts(to_ftsdoc('fox'), 'fox'::ftsquery, 1000, 5.0, 1.2, 0.75, 'bm25+', ARRAY[3.0])
     > fts_bm25_opts(to_ftsdoc('fox'), 'fox'::ftsquery, 1000, 5.0, 1.2, 0.75, 'lucene', ARRAY[3.0])
       AS bm25plus_ge_lucene;
-- unknown variant errors
SELECT fts_bm25_opts(to_ftsdoc('x'), 'x'::ftsquery, 10, 1.0, 1.2, 0.75, 'bogus');

-- Stage 8: highlight and snippet.
ALTER EXTENSION pg_fts UPDATE TO '1.5';
SELECT fts_highlight('The quick brown fox jumped', 'quick | fox'::ftsquery,
                     '[', ']');
SELECT fts_snippet(
  'lorem ipsum dolor the quick brown fox jumps over the lazy dog etcetera etc',
  'quick & fox'::ftsquery, '<', '>', '...', 6);
-- no match: highlight returns the text unchanged
SELECT fts_highlight('nothing here matches', 'zebra'::ftsquery, '[', ']');

-- Stage 11: migration from tsquery.
ALTER EXTENSION pg_fts UPDATE TO '1.6';
-- boolean operators convert directly
SELECT tsquery_to_ftsquery('quick & brown'::tsquery);
SELECT tsquery_to_ftsquery('quick | brown'::tsquery);
SELECT tsquery_to_ftsquery('!slow & quick'::tsquery);
SELECT tsquery_to_ftsquery('(a | b) & !c'::tsquery);
-- phrase degrades to AND with a NOTICE
SELECT tsquery_to_ftsquery('quick <-> brown'::tsquery);
-- the tsquery -> ftsquery cast makes existing queries usable with @@@
SELECT to_ftsdoc('the quick brown fox') @@@ ('quick & fox'::tsquery)::ftsquery
       AS migrated_match;

-- Stage 6 (partial): prefix queries (term*).
SELECT 'quick*'::ftsquery;                        -- renders with the star
SELECT to_ftsdoc('the quicksand shifts') @@@ 'quick*'::ftsquery AS prefix_hit;
SELECT to_ftsdoc('slow and steady') @@@ 'quick*'::ftsquery AS prefix_miss;
SELECT to_ftsdoc('quick brown fox') @@@ 'qu* & fo*'::ftsquery AS prefix_and;
-- prefix works through the bm25 index too
CREATE TABLE pfx (id serial, d ftsdoc);
INSERT INTO pfx (d) VALUES (to_ftsdoc('quicksand')), (to_ftsdoc('quiche')),
                          (to_ftsdoc('slow'));
CREATE INDEX pfx_bm25 ON pfx USING bm25 (d);
SET enable_seqscan = off;
SELECT id FROM pfx WHERE d @@@ 'qui*'::ftsquery ORDER BY id;
RESET enable_seqscan;
DROP TABLE pfx;

-- Stage 5: index-maintained corpus statistics for BM25.
ALTER EXTENSION pg_fts UPDATE TO '1.7';
CREATE TABLE corpus (id serial, d ftsdoc);
INSERT INTO corpus (d)
SELECT to_ftsdoc('common ' || CASE WHEN g % 10 = 0 THEN 'rare' ELSE 'filler' END)
FROM generate_series(1, 100) g;
CREATE INDEX corpus_bm25 ON corpus USING bm25 (d);
-- stats reflect the corpus: 100 docs
SELECT ndocs, nterms FROM fts_index_stats('corpus_bm25');
-- 'rare' (df=10) scores higher than 'common' (df=100) using index df
SELECT fts_index_df('corpus_bm25', 'rare'::ftsquery) AS df_rare,
       fts_index_df('corpus_bm25', 'common'::ftsquery) AS df_common;
SELECT (SELECT fts_bm25(to_ftsdoc('common rare'), 'rare'::ftsquery,
                        s.ndocs, s.avgdl, fts_index_df('corpus_bm25', 'rare'::ftsquery)))
     > (SELECT fts_bm25(to_ftsdoc('common rare'), 'common'::ftsquery,
                        s.ndocs, s.avgdl, fts_index_df('corpus_bm25', 'common'::ftsquery)))
       AS rare_outranks_common
FROM fts_index_stats('corpus_bm25') s;
DROP TABLE corpus;

-- Stage 7: incremental index maintenance (pending list).
ALTER EXTENSION pg_fts UPDATE TO '1.8';
CREATE TABLE inc (id serial, d ftsdoc);
INSERT INTO inc (d) VALUES (to_ftsdoc('alpha beta')), (to_ftsdoc('gamma delta'));
CREATE INDEX inc_bm25 ON inc USING bm25 (d);
SET enable_seqscan = off;
-- rows present at build time are found via the main structure
SELECT id FROM inc WHERE d @@@ 'alpha'::ftsquery ORDER BY id;
-- INSERT after build must be immediately visible (no REINDEX) via pending list
INSERT INTO inc (d) VALUES (to_ftsdoc('alpha epsilon')), (to_ftsdoc('zeta'));
SELECT id FROM inc WHERE d @@@ 'alpha'::ftsquery ORDER BY id;   -- 1 and 3
SELECT id FROM inc WHERE d @@@ 'zeta'::ftsquery ORDER BY id;     -- 4 (pending only)
SELECT id FROM inc WHERE d @@@ 'alpha & !beta'::ftsquery ORDER BY id;  -- 3
-- ndocs reflects built + pending
SELECT ndocs FROM fts_index_stats('inc_bm25');
-- REINDEX merges pending into the main structure; results unchanged
REINDEX INDEX inc_bm25;
SELECT id FROM inc WHERE d @@@ 'alpha'::ftsquery ORDER BY id;
RESET enable_seqscan;
DROP TABLE inc;

-- Stage 6 (phrase): quoted phrase queries via per-term positions.
ALTER EXTENSION pg_fts UPDATE TO '1.9';
-- phrase renders with <-> and round-trips
SELECT '"quick brown fox"'::ftsquery;
-- adjacency is enforced: "quick brown" matches, "quick fox" does not
SELECT to_ftsdoc('the quick brown fox') @@@ '"quick brown"'::ftsquery AS adj_hit;
SELECT to_ftsdoc('the quick brown fox') @@@ '"quick fox"'::ftsquery AS adj_miss;
SELECT to_ftsdoc('the quick brown fox') @@@ '"brown fox"'::ftsquery AS adj_hit2;
-- word order matters: "fox brown" does not match "...brown fox"
SELECT to_ftsdoc('the quick brown fox') @@@ '"fox brown"'::ftsquery AS order_miss;
-- three-word phrase
SELECT to_ftsdoc('the quick brown fox jumps') @@@ '"quick brown fox"'::ftsquery AS three_hit;
SELECT to_ftsdoc('quick red brown fox') @@@ '"quick brown fox"'::ftsquery AS three_miss;
-- phrase combined with boolean operators
SELECT to_ftsdoc('the quick brown fox') @@@ '"quick brown" & fox'::ftsquery AS combo;
-- phrase works through the bm25 index (recheck enforces adjacency)
CREATE TABLE ph (id serial, d ftsdoc);
INSERT INTO ph (d) VALUES (to_ftsdoc('quick brown fox')),
                          (to_ftsdoc('brown quick fox')),
                          (to_ftsdoc('quick brown bear'));
CREATE INDEX ph_bm25 ON ph USING bm25 (d);
SET enable_seqscan = off;
SELECT id FROM ph WHERE d @@@ '"quick brown"'::ftsquery ORDER BY id;   -- 1 and 3
RESET enable_seqscan;
DROP TABLE ph;

-- Stage 10: external-content indexing via an expression index.
-- The bm25 index stores only postings (no document text), so indexing
-- to_ftsdoc(body) over a plain text column is the external-content model:
-- the text lives in the table, the index derives ftsdoc from it.
ALTER EXTENSION pg_fts UPDATE TO '1.10';
CREATE TABLE articles (id serial, body text);
INSERT INTO articles (body) VALUES
  ('the quick brown fox'),
  ('lazy dogs sleep'),
  ('quick foxes are clever');
CREATE INDEX articles_bm25 ON articles USING bm25 (to_ftsdoc(body));
SET enable_seqscan = off;
-- query against the expression index; text is fetched from the table only
-- for returned rows
SELECT id, body FROM articles
WHERE to_ftsdoc(body) @@@ 'quick'::ftsquery ORDER BY id;
SELECT id FROM articles
WHERE to_ftsdoc(body) @@@ '"quick brown"'::ftsquery ORDER BY id;
RESET enable_seqscan;
DROP TABLE articles;

-- Stages 13-14: fuzzy (term~k) and regex (/re/) queries.
ALTER EXTENSION pg_fts UPDATE TO '1.11';
-- fuzzy: 'quick'~1 matches 'quikc'? no (2 edits); matches 'quic' (1 delete)
SELECT to_ftsdoc('the quic brown fox') @@@ 'quick~1'::ftsquery AS fuzzy_hit;
SELECT to_ftsdoc('the slow green turtle') @@@ 'quick~1'::ftsquery AS fuzzy_miss;
-- default k is 2: 'kwik' is 3 edits from 'quick', so 'quick~' (k=2) misses
SELECT to_ftsdoc('kwik search') @@@ 'quick~'::ftsquery AS fuzzy_default_k;
-- fuzzy renders with ~k
SELECT 'color~2'::ftsquery;
-- regex: /^qu/ matches a term starting with qu
SELECT to_ftsdoc('the quick brown fox') @@@ '/^qu/'::ftsquery AS regex_hit;
SELECT to_ftsdoc('lazy dog') @@@ '/^qu/'::ftsquery AS regex_miss;
-- regex renders with slashes
SELECT '/ab.*cd/'::ftsquery;
-- fuzzy combined with boolean
SELECT to_ftsdoc('the quic brown fox') @@@ 'quick~1 & fox'::ftsquery AS combo;
-- fuzzy/regex work through the bm25 index (recheck applies the exact test)
CREATE TABLE fz (id serial, d ftsdoc);
INSERT INTO fz (d) VALUES (to_ftsdoc('quick')), (to_ftsdoc('quic')),
                          (to_ftsdoc('slow'));
CREATE INDEX fz_bm25 ON fz USING bm25 (d);
SET enable_seqscan = off;
SELECT id FROM fz WHERE d @@@ 'quick~1'::ftsquery ORDER BY id;   -- 1 and 2
SELECT id FROM fz WHERE d @@@ '/^qu/'::ftsquery ORDER BY id;      -- 1 and 2
RESET enable_seqscan;
DROP TABLE fz;

-- Stage 3: the bm25 index access method.
ALTER EXTENSION pg_fts UPDATE TO '1.3';

CREATE TABLE idxdocs (id serial, d ftsdoc);
INSERT INTO idxdocs (d) VALUES
  (to_ftsdoc('the quick brown fox')),
  (to_ftsdoc('a slow green turtle')),
  (to_ftsdoc('quick turtles are rare')),
  (to_ftsdoc('brown bears and quick foxes'));

CREATE INDEX idxdocs_bm25 ON idxdocs USING bm25 (d);

-- force index usage and confirm the plan uses a bitmap scan on our AM
SET enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT id FROM idxdocs WHERE d @@@ 'quick'::ftsquery ORDER BY id;

-- results must match a sequential @@@ evaluation
SELECT id FROM idxdocs WHERE d @@@ 'quick'::ftsquery ORDER BY id;
SELECT id FROM idxdocs WHERE d @@@ 'quick & fox'::ftsquery ORDER BY id;
SELECT id FROM idxdocs WHERE d @@@ 'quick | slow'::ftsquery ORDER BY id;
SELECT id FROM idxdocs WHERE d @@@ 'quick & !fox'::ftsquery ORDER BY id;
SELECT id FROM idxdocs WHERE d @@@ '!turtle'::ftsquery ORDER BY id;
SELECT id FROM idxdocs WHERE d @@@ '(quick | slow) & !fox'::ftsquery ORDER BY id;
RESET enable_seqscan;

DROP TABLE idxdocs;
