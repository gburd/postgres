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
FROM unnest(ARRAY['lucene','robertson','atire','bm25+','bm25l']) AS variant
ORDER BY variant;
-- bm25+ >= lucene for the same inputs (delta floor)
SELECT fts_bm25_opts(to_ftsdoc('fox'), 'fox'::ftsquery, 1000, 5.0, 1.2, 0.75, 'bm25+', ARRAY[3.0])
     > fts_bm25_opts(to_ftsdoc('fox'), 'fox'::ftsquery, 1000, 5.0, 1.2, 0.75, 'lucene', ARRAY[3.0])
       AS bm25plus_ge_lucene;
-- bm25l (rank_bm25 compatible: delta shift on the length-normalized tf) scores
-- a present term positively and 'l' is an accepted alias for it
SELECT fts_bm25_opts(to_ftsdoc('fox fox fox'), 'fox'::ftsquery, 1000, 5.0, 1.5, 0.75, 'bm25l', ARRAY[3.0]) > 0 AS bm25l_positive,
       fts_bm25_opts(to_ftsdoc('fox fox fox'), 'fox'::ftsquery, 1000, 5.0, 1.5, 0.75, 'bm25l', ARRAY[3.0])
     = fts_bm25_opts(to_ftsdoc('fox fox fox'), 'fox'::ftsquery, 1000, 5.0, 1.5, 0.75, 'l', ARRAY[3.0]) AS l_alias;
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
-- phrase operator <-> converts faithfully to an ftsquery phrase
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

-- BM25F: multi-field weighting.
ALTER EXTENSION pg_fts UPDATE TO '1.12';
-- a term in the (heavily weighted) title scores higher than the same term in
-- only the body
SELECT fts_bm25f(ARRAY[to_ftsdoc('postgres'), to_ftsdoc('other text here')],
                'postgres'::ftsquery, ARRAY[5.0, 1.0], 1000, ARRAY[2.0, 3.0], ARRAY[10.0])
     > fts_bm25f(ARRAY[to_ftsdoc('other title'), to_ftsdoc('postgres here')],
                'postgres'::ftsquery, ARRAY[5.0, 1.0], 1000, ARRAY[2.0, 3.0], ARRAY[10.0])
       AS title_weight_wins;
-- absent term scores 0
SELECT fts_bm25f(ARRAY[to_ftsdoc('a'), to_ftsdoc('b')],
                'zebra'::ftsquery, ARRAY[2.0, 1.0], 100, ARRAY[1.0, 1.0]) AS absent_zero;
-- a match in either field contributes
SELECT fts_bm25f(ARRAY[to_ftsdoc('nothing'), to_ftsdoc('found fox')],
                'fox'::ftsquery, ARRAY[2.0, 1.0], 100, ARRAY[2.0, 2.0], ARRAY[5.0]) > 0
       AS body_match_scores;
-- mismatched array lengths error
SELECT fts_bm25f(ARRAY[to_ftsdoc('a')], 'a'::ftsquery, ARRAY[1.0,2.0], 10, ARRAY[1.0]);

-- Background merge of the pending list.
ALTER EXTENSION pg_fts UPDATE TO '1.13';
CREATE TABLE mrg (id serial, d ftsdoc);
INSERT INTO mrg (d) VALUES (to_ftsdoc('alpha beta'));
CREATE INDEX mrg_bm25 ON mrg USING bm25 (d);
INSERT INTO mrg (d) VALUES (to_ftsdoc('alpha gamma')), (to_ftsdoc('delta'));
-- before merge: 2 docs pending
SELECT ndocs FROM fts_index_stats('mrg_bm25');
SET enable_seqscan = off;
SELECT id FROM mrg WHERE d @@@ 'alpha'::ftsquery ORDER BY id;   -- 1, 2
-- explicit merge folds pending into the main structure
SELECT fts_merge('mrg_bm25') AS merged;
-- after merge: same results, and a term from a formerly-pending doc now has df
SELECT id FROM mrg WHERE d @@@ 'alpha'::ftsquery ORDER BY id;   -- still 1, 2
SELECT id FROM mrg WHERE d @@@ 'delta'::ftsquery ORDER BY id;   -- 3
SELECT fts_index_df('mrg_bm25', 'alpha'::ftsquery) AS df_alpha_after_merge;
-- merging again is a no-op (nothing pending)
SELECT fts_merge('mrg_bm25') AS merged_again;
RESET enable_seqscan;
DROP TABLE mrg;

-- Index-only BM25 top-k search (fts_search).
ALTER EXTENSION pg_fts UPDATE TO '1.14';
CREATE TABLE srch (id serial, body text, d ftsdoc);
INSERT INTO srch (body, d) VALUES
  ('quick quick quick fox', to_ftsdoc('quick quick quick fox')),
  ('quick brown fox', to_ftsdoc('quick brown fox')),
  ('a slow turtle', to_ftsdoc('a slow turtle')),
  ('quick', to_ftsdoc('quick'));
CREATE INDEX srch_bm25 ON srch USING bm25 (d);
-- top-k by index-only score: doc with tf(quick)=3 ranks first
SELECT s.id, round(r.score::numeric, 3) AS score
FROM fts_search('srch_bm25', 'quick'::ftsquery, 10) r
JOIN srch s ON s.ctid = r.ctid
ORDER BY r.score DESC, s.id;
-- k limits the result set
SELECT count(*) AS topk_count
FROM fts_search('srch_bm25', 'quick'::ftsquery, 2) r;
-- multi-term query accumulates per-term contributions
SELECT count(*) AS multiterm
FROM fts_search('srch_bm25', 'quick | brown'::ftsquery, 10) r;
DROP TABLE srch;

-- Trigram pre-filter for fuzzy matching: correctness must be unchanged.
ALTER EXTENSION pg_fts UPDATE TO '1.15';
-- longer terms (>k trigrams) use the trigram filter
SELECT to_ftsdoc('development environment') @@@ 'developer~3'::ftsquery AS trgm_fuzzy_hit;
SELECT to_ftsdoc('completely unrelated words') @@@ 'developer~3'::ftsquery AS trgm_fuzzy_miss;
-- exact-distance edge: 'running' within 2 of 'runnick'? (n->c,g->k = 2 subst)
SELECT to_ftsdoc('the running man') @@@ 'runnink~2'::ftsquery AS edge_hit;
-- short terms (<=k trigrams) fall back to full scan, still correct
SELECT to_ftsdoc('cat hat bat') @@@ 'rat~1'::ftsquery AS short_hit;
SELECT to_ftsdoc('dog log fog') @@@ 'rat~1'::ftsquery AS short_miss;

-- MVCC: fts_search must return only tuples visible to the snapshot.
CREATE TABLE viz (id serial, d ftsdoc);
INSERT INTO viz (d) VALUES (to_ftsdoc('apple')), (to_ftsdoc('apple')),
                          (to_ftsdoc('apple')), (to_ftsdoc('apple'));
CREATE INDEX viz_bm25 ON viz USING bm25 (d);
-- delete two rows; their postings remain in the index until merge/reindex
DELETE FROM viz WHERE id IN (2, 3);
-- fts_search must skip the dead tuples (returns 2 live rows, not 4)
SELECT count(*) AS live_only
FROM fts_search('viz_bm25', 'apple'::ftsquery, 100) r
JOIN viz v ON v.ctid = r.ctid;
-- and the raw SRF itself returns only visible ctids
SELECT count(*) AS srf_live FROM fts_search('viz_bm25', 'apple'::ftsquery, 100);
DROP TABLE viz;

-- Posting compression: correctness under many clustered docids + merge.
CREATE TABLE cmp (id serial, d ftsdoc);
INSERT INTO cmp (d) SELECT to_ftsdoc('common term here')
FROM generate_series(1, 500);
CREATE INDEX cmp_bm25 ON cmp USING bm25 (d);
-- all 500 rows match the compressed posting list
SELECT count(*) AS all_match
FROM fts_search('cmp_bm25', 'common'::ftsquery, 1000) r JOIN cmp c ON c.ctid = r.ctid;
-- incremental inserts (pending) + merge preserve the full posting list
INSERT INTO cmp (d) SELECT to_ftsdoc('common term here') FROM generate_series(1, 100);
SELECT fts_merge('cmp_bm25');
SELECT count(*) AS after_merge
FROM fts_search('cmp_bm25', 'common'::ftsquery, 2000) r JOIN cmp c ON c.ctid = r.ctid;
DROP TABLE cmp;

-- WAND top-k: multi-term query returns correct top-k in descending score.
CREATE TABLE wnd (id serial, d ftsdoc);
INSERT INTO wnd (d) VALUES
  (to_ftsdoc('alpha alpha alpha beta')),   -- high alpha tf
  (to_ftsdoc('alpha beta beta beta')),     -- high beta tf
  (to_ftsdoc('alpha beta')),               -- both, low tf
  (to_ftsdoc('gamma only')),               -- neither
  (to_ftsdoc('alpha')),                    -- alpha only
  (to_ftsdoc('beta'));                     -- beta only
CREATE INDEX wnd_bm25 ON wnd USING bm25 (d);
-- top-2 for 'alpha | beta': the two docs matching both terms should lead
SELECT w.id
FROM fts_search('wnd_bm25', 'alpha | beta'::ftsquery, 3) r
JOIN wnd w ON w.ctid = r.ctid
ORDER BY r.score DESC, w.id;
-- scores are monotonically non-increasing (WAND returns them sorted)
SELECT bool_and(s >= lead_s) AS descending
FROM (SELECT r.score AS s, lead(r.score) OVER (ORDER BY r.score DESC) AS lead_s
      FROM fts_search('wnd_bm25', 'alpha | beta'::ftsquery, 10) r) q
WHERE lead_s IS NOT NULL;
DROP TABLE wnd;

-- Lazy block-max WAND: correct top-k over a multi-page posting list.
CREATE TABLE lazy (id serial, d ftsdoc);
-- 2000 docs of 'term': posting list spans many pages; one doc has high tf
INSERT INTO lazy (d) SELECT to_ftsdoc('term') FROM generate_series(1, 2000);
INSERT INTO lazy (d) VALUES (to_ftsdoc('term term term term term'));  -- id 2001
CREATE INDEX lazy_bm25 ON lazy USING bm25 (d);
-- top-1 must be the high-tf doc (id 2001), found via block-max skipping
SELECT l.id
FROM fts_search('lazy_bm25', 'term'::ftsquery, 1) r JOIN lazy l ON l.ctid = r.ctid;
-- top-3 all correct and the whole list is searchable
SELECT count(*) AS matched
FROM fts_search('lazy_bm25', 'term'::ftsquery, 5000) r JOIN lazy l ON l.ctid = r.ctid;
DROP TABLE lazy;

-- NEAR(a b, k): proximity within k tokens.
-- 'quick ... fox' are 3 tokens apart in 'the quick brown red fox'
SELECT to_ftsdoc('the quick brown red fox') @@@ 'NEAR(quick fox, 3)'::ftsquery AS near_hit;
SELECT to_ftsdoc('the quick brown red fox') @@@ 'NEAR(quick fox, 2)'::ftsquery AS near_miss;
-- adjacent terms satisfy any k>=1
SELECT to_ftsdoc('the quick brown fox') @@@ 'NEAR(quick brown, 1)'::ftsquery AS near_adj;
-- three-term NEAR chains the proximity
SELECT to_ftsdoc('alpha beta gamma delta') @@@ 'NEAR(alpha beta gamma, 2)'::ftsquery AS near3;
-- NEAR combines with boolean operators
SELECT to_ftsdoc('the quick brown red fox jumps') @@@ 'NEAR(quick fox, 3) & jumps'::ftsquery AS near_combo;
-- malformed NEAR errors (single term); NEAR without k defaults to 10
SELECT 'NEAR(onlyone, 2)'::ftsquery;
SELECT to_ftsdoc('a b c') @@@ 'NEAR(a c)'::ftsquery AS near_default_k;

-- FSM page recycling: repeated merges reuse freed blocks (no unbounded growth).
CREATE TABLE recyc (id serial, d ftsdoc);
INSERT INTO recyc (d) SELECT to_ftsdoc('term' || (g % 50)) FROM generate_series(1, 500) g;
CREATE INDEX recyc_bm25 ON recyc USING bm25 (d);
-- churn: insert + merge several times; each merge frees the old blocks
DO $$
BEGIN
  FOR i IN 1..5 LOOP
    INSERT INTO recyc (d) SELECT to_ftsdoc('term' || (g % 50)) FROM generate_series(1, 200) g;
    PERFORM fts_merge('recyc_bm25');
  END LOOP;
END $$;
-- after churn the index is still correct
SELECT count(*) > 0 AS still_matches
FROM fts_search('recyc_bm25', 'term1'::ftsquery, 5000) r JOIN recyc x ON x.ctid = r.ctid;
-- size stays bounded across churn (freed blocks recycled, not leaked); the
-- bound includes the trigram index pages rebuilt on each merge.
SELECT pg_relation_size('recyc_bm25') < 800 * 8192 AS size_bounded;
DROP TABLE recyc;

-- amcanorderbyop: ORDER BY col <=> query LIMIT k uses an index ordering scan.
ALTER EXTENSION pg_fts UPDATE TO '1.16';
CREATE TABLE ord (id serial, d ftsdoc);
INSERT INTO ord (d) VALUES
  (to_ftsdoc('quick quick quick fox')),   -- highest tf(quick)
  (to_ftsdoc('quick brown fox')),
  (to_ftsdoc('a slow turtle')),
  (to_ftsdoc('quick'));                    -- short doc, high length-norm score
CREATE INDEX ord_bm25 ON ord USING bm25 (d);
SET enable_seqscan = off;
-- the plan should be an index scan ordered by the distance operator (no Sort)
EXPLAIN (COSTS OFF)
SELECT id FROM ord WHERE d @@@ 'quick'::ftsquery
ORDER BY d <=> 'quick'::ftsquery LIMIT 2;
-- results ordered by relevance (ascending distance)
SELECT id FROM ord WHERE d @@@ 'quick'::ftsquery
ORDER BY d <=> 'quick'::ftsquery LIMIT 3;
RESET enable_seqscan;
DROP TABLE ord;

-- On-disk trigram index: fuzzy/regex through the index over many docs.
-- The trigram funnel narrows candidates (sparsemap postings); recheck refines.
CREATE TABLE trgm (id serial, d ftsdoc);
INSERT INTO trgm (d) SELECT to_ftsdoc('document' || g) FROM generate_series(1, 1000) g;
INSERT INTO trgm (d) VALUES (to_ftsdoc('documemt42'));   -- id 1001, 1 edit from document42
CREATE INDEX trgm_bm25 ON trgm USING bm25 (d);
SET enable_seqscan = off;
-- fuzzy: finds the exact 'document42' (id 43: 'document42') and the typo (1001)
SELECT count(*) AS fuzzy_via_trigram
FROM trgm t WHERE t.d @@@ 'document42~2'::ftsquery;
-- regex through the trigram index
SELECT count(*) AS regex_via_trigram
FROM trgm t WHERE t.d @@@ '/document4[0-9]$/'::ftsquery;
-- regex with alternation/anchors: literal-run tiling extracts 'document'
SELECT count(*) AS regex_anchored
FROM trgm t WHERE t.d @@@ '/^document(4|5)2$/'::ftsquery;   -- document42, document52
RESET enable_seqscan;
DROP TABLE trgm;

-- Adaptive-k ordering scan: consuming more than the initial batch (64) must
-- grow k and return the full ordered set correctly across the boundary.
CREATE TABLE bigord (id serial, d ftsdoc);
INSERT INTO bigord (d) SELECT to_ftsdoc('term extra' || (g % 3)) FROM generate_series(1, 300) g;
CREATE INDEX bigord_bm25 ON bigord USING bm25 (d);
SET enable_seqscan = off;
-- all 300 match 'term'; requesting 200 crosses the initial k=64 batch
SELECT count(*) AS got, bool_and(s >= lead_s) AS ordered_ok
FROM (SELECT r.score AS s, lead(r.score) OVER (ORDER BY r.score DESC) AS lead_s
      FROM (SELECT id, d <=> 'term'::ftsquery AS score FROM bigord
            WHERE d @@@ 'term'::ftsquery
            ORDER BY d <=> 'term'::ftsquery LIMIT 200) r) q;
RESET enable_seqscan;
DROP TABLE bigord;

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

-- Config-normalized query: to_ftsquery(regconfig, text) must stem query terms
-- to match a doc indexed with the same config (the EC2 benchmark fault).
ALTER EXTENSION pg_fts UPDATE TO '1.17';
-- 'postgres' stems to 'postgr'; a raw ftsquery misses, a config query matches
SELECT to_ftsdoc('english'::regconfig, 'postgres databases') @@@ 'postgres'::ftsquery
       AS raw_query_misses;
SELECT to_ftsdoc('english'::regconfig, 'postgres databases')
       @@@ to_ftsquery('english'::regconfig, 'postgres')
       AS config_query_matches;
-- multi-term config query with operators
SELECT to_ftsdoc('english'::regconfig, 'running quickly through fields')
       @@@ to_ftsquery('english'::regconfig, 'run & quick')
       AS stemmed_and;
-- config query renders the stemmed terms
SELECT to_ftsquery('english'::regconfig, 'databases running')::text AS stemmed_render;

-- Segmented architecture: queries must span multiple segments correctly.
CREATE TABLE seg (id serial, d ftsdoc);
INSERT INTO seg(d) SELECT to_ftsdoc('alpha doc'||g) FROM generate_series(1,100) g;
CREATE INDEX seg_bm25 ON seg USING bm25 (d);          -- segment 0
INSERT INTO seg(d) SELECT to_ftsdoc('beta doc'||g) FROM generate_series(1,50) g;
SELECT fts_merge('seg_bm25');                          -- flush -> segment 1
INSERT INTO seg(d) SELECT to_ftsdoc('alpha more'||g) FROM generate_series(1,30) g;
SELECT fts_merge('seg_bm25');                          -- flush -> segment 2
SET enable_seqscan = off;
SELECT count(*) AS alpha_spans_segs FROM seg WHERE d @@@ 'alpha'::ftsquery;  -- 130
SELECT count(*) AS beta_one_seg FROM seg WHERE d @@@ 'beta'::ftsquery;        -- 50
SELECT ndocs AS total_docs FROM fts_index_stats('seg_bm25');                  -- 180
SELECT count(*) AS ranked_across_segs
FROM (SELECT id FROM seg WHERE d @@@ 'alpha'::ftsquery
      ORDER BY d <=> 'alpha'::ftsquery LIMIT 5) x;                            -- 5
RESET enable_seqscan;
DROP TABLE seg;

-- Tiered merge: many flushes create many segments; merge compacts them so the
-- segment count stays bounded while results are preserved.
ALTER EXTENSION pg_fts UPDATE TO '1.18';
CREATE TABLE tier (id serial, d ftsdoc);
INSERT INTO tier(d) SELECT to_ftsdoc('common w'||(g%20)) FROM generate_series(1,100) g;
CREATE INDEX tier_bm25 ON tier USING bm25 (d);
DO $$ BEGIN FOR i IN 1..10 LOOP
  INSERT INTO tier(d) SELECT to_ftsdoc('common x'||(g%20)) FROM generate_series(1,20) g;
  PERFORM fts_merge('tier_bm25');
END LOOP; END $$;
SET enable_seqscan = off;
SELECT count(*) AS all_docs FROM tier WHERE d @@@ 'common'::ftsquery;   -- 300
SELECT ndocs FROM fts_index_stats('tier_bm25');                          -- 300
SELECT fts_index_nsegments('tier_bm25') <= 8 AS segments_bounded;        -- t (compacted)
RESET enable_seqscan;
DROP TABLE tier;

-- FOR-128 block posting codec: a term spanning many 128-doc blocks and pages
-- decodes correctly and block-max WAND top-k matches a full scan+sort.
CREATE TABLE blk (id serial, d ftsdoc);
INSERT INTO blk(d) SELECT to_ftsdoc('pop '||repeat('pop ',(g%7))||'w'||g)
  FROM generate_series(1,3000) g;      -- 'pop' in 3000 docs, >20 blocks/page
CREATE INDEX blk_bm25 ON blk USING bm25 (d);
SET enable_seqscan = off;
SELECT count(*) AS pop_ct FROM blk WHERE d @@@ 'pop'::ftsquery;   -- 3000
-- WAND top-10 distances equal a full seqscan sort's top-10 distances
CREATE TEMP TABLE w AS SELECT round((d <=> 'pop'::ftsquery)::numeric,6) dist
  FROM blk WHERE d @@@ 'pop'::ftsquery ORDER BY dist LIMIT 10;
SET enable_indexscan = off; SET enable_bitmapscan = off; SET enable_seqscan = on;
CREATE TEMP TABLE f AS SELECT round((d <=> 'pop'::ftsquery)::numeric,6) dist
  FROM blk WHERE d @@@ 'pop'::ftsquery ORDER BY dist LIMIT 10;
SELECT (SELECT array_agg(dist ORDER BY dist) FROM w)
     = (SELECT array_agg(dist ORDER BY dist) FROM f) AS wand_scores_match_fullsort;
RESET enable_seqscan; RESET enable_indexscan; RESET enable_bitmapscan;
DROP TABLE blk;

-- Sparse dictionary block index (FST-equivalent point lookup): a many-page
-- dictionary routes exact lookups to the right page; first/middle/last/absent
-- terms all resolve correctly, and df via the seek path is exact.
CREATE TABLE voc (id serial, d ftsdoc);
INSERT INTO voc(d) SELECT to_ftsdoc('term'||lpad(g::text,5,'0')||' shared')
  FROM generate_series(1,8000) g;      -- 8000 distinct terms -> multi-page dict
CREATE INDEX voc_bm25 ON voc USING bm25 (d);
SET enable_seqscan = off;
SELECT count(*) AS first_term  FROM voc WHERE d @@@ 'term00001'::ftsquery;  -- 1
SELECT count(*) AS mid_term    FROM voc WHERE d @@@ 'term04000'::ftsquery;  -- 1
SELECT count(*) AS last_term   FROM voc WHERE d @@@ 'term08000'::ftsquery;  -- 1
SELECT count(*) AS absent_term FROM voc WHERE d @@@ 'termzzzzz'::ftsquery;  -- 0
SELECT count(*) AS shared_all  FROM voc WHERE d @@@ 'shared'::ftsquery;     -- 8000
SELECT fts_index_df('voc_bm25','term04000'::ftsquery) AS df_mid;            -- {1}
-- block index survives an insert+flush (new segment gets its own index)
INSERT INTO voc(d) SELECT to_ftsdoc('term'||lpad(g::text,5,'0')||' shared')
  FROM generate_series(8001,8100) g;
SELECT fts_merge('voc_bm25');
SELECT count(*) AS after_flush FROM voc WHERE d @@@ 'term08050'::ftsquery;  -- 1
RESET enable_seqscan;
DROP TABLE voc;

-- Block-Max WAND (BMW): the block-max skip prunes whole 128-blocks that cannot
-- beat the current top-k threshold, and the exact top-k is unchanged.
CREATE TABLE bmw (id serial, d ftsdoc);
INSERT INTO bmw(d) SELECT to_ftsdoc('alpha '||
    CASE WHEN g%50=0 THEN repeat('beta ',5) ELSE '' END||'w'||g)
  FROM generate_series(1,12000) g;
CREATE INDEX bmw_bm25 ON bmw USING bm25 (d);
SET enable_seqscan = off;
CREATE TEMP TABLE bmw_top AS SELECT round((d <=> 'alpha beta'::ftsquery)::numeric,6) dist
  FROM bmw WHERE d @@@ 'alpha beta'::ftsquery ORDER BY dist LIMIT 10;
SET enable_indexscan = off; SET enable_bitmapscan = off; SET enable_seqscan = on;
CREATE TEMP TABLE bmw_all AS SELECT round((d <=> 'alpha beta'::ftsquery)::numeric,6) dist
  FROM bmw WHERE d @@@ 'alpha beta'::ftsquery ORDER BY dist LIMIT 10;
SELECT (SELECT array_agg(dist ORDER BY dist) FROM bmw_top)
     = (SELECT array_agg(dist ORDER BY dist) FROM bmw_all) AS bmw_exact_topk;
RESET enable_seqscan; RESET enable_indexscan; RESET enable_bitmapscan;
DROP TABLE bmw;

-- Levenshtein-automaton fuzzy: matches EXACTLY the dictionary terms within k
-- edits (no trigram over-generation), verified against core levenshtein.
CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;
CREATE TABLE fz (id serial, body text, d ftsdoc);
INSERT INTO fz(body) SELECT 'document'||g||' filler' FROM generate_series(1,5000) g;
UPDATE fz SET d = to_ftsdoc(body);
CREATE INDEX fz_bm25 ON fz USING bm25 (d);
SET enable_seqscan = off;
SELECT count(*) AS dfa_fuzzy FROM fz WHERE d @@@ 'document42~2'::ftsquery;
SET enable_seqscan = on; SET enable_indexscan = off; SET enable_bitmapscan = off;
SELECT count(*) AS ground_truth FROM fz
WHERE EXISTS (SELECT 1 FROM unnest(string_to_array(body,' ')) t
              WHERE levenshtein_less_equal(t,'document42',2) <= 2);
RESET enable_seqscan; RESET enable_indexscan; RESET enable_bitmapscan;
DROP TABLE fz;
DROP EXTENSION fuzzystrmatch;

-- MaxScore top-k (chosen for queries with >=4 terms): identical exact top-k to
-- a full scan+sort, doing less work as low-impact terms become non-essential.
CREATE TABLE ms (id serial, d ftsdoc);
INSERT INTO ms(d) SELECT to_ftsdoc(
  (CASE WHEN g%2=0 THEN 'alpha ' ELSE '' END)||
  (CASE WHEN g%3=0 THEN 'beta ' ELSE '' END)||
  (CASE WHEN g%5=0 THEN 'gamma ' ELSE '' END)||
  (CASE WHEN g%7=0 THEN 'delta ' ELSE '' END)||
  (CASE WHEN g%11=0 THEN 'epsilon ' ELSE '' END)||'w'||g)
  FROM generate_series(1,20000) g;
CREATE INDEX ms_bm25 ON ms USING bm25 (d);
SET enable_seqscan = off;
CREATE TEMP TABLE mtop AS SELECT round((d <=> 'alpha beta gamma delta epsilon'::ftsquery)::numeric,6) dist
  FROM ms WHERE d @@@ 'alpha beta gamma delta epsilon'::ftsquery ORDER BY dist LIMIT 20;
SET enable_indexscan = off; SET enable_bitmapscan = off; SET enable_seqscan = on;
CREATE TEMP TABLE mall AS SELECT round((d <=> 'alpha beta gamma delta epsilon'::ftsquery)::numeric,6) dist
  FROM ms WHERE d @@@ 'alpha beta gamma delta epsilon'::ftsquery ORDER BY dist LIMIT 20;
SELECT (SELECT array_agg(dist ORDER BY dist) FROM mtop)
     = (SELECT array_agg(dist ORDER BY dist) FROM mall) AS maxscore_exact_topk;
RESET enable_seqscan; RESET enable_indexscan; RESET enable_bitmapscan;
DROP TABLE ms;

-- Tombstones: VACUUM must remove deleted docs from the index so the index-only
-- scan and fts_count (which trust the visibility map) never report a
-- vacuumed-and-reused heap slot, and a later merge must physically drop them.
CREATE EXTENSION IF NOT EXISTS pg_fts;
ALTER EXTENSION pg_fts UPDATE TO '1.19';
CREATE TABLE tomb (id int primary key, d ftsdoc);
INSERT INTO tomb SELECT g, to_ftsdoc('alpha doc'||g) FROM generate_series(1,100) g;
CREATE INDEX tomb_bm25 ON tomb USING bm25 (d);
SET enable_seqscan = off;
SELECT count(*) AS c_before FROM tomb WHERE d @@@ 'alpha'::ftsquery;         -- 100
DELETE FROM tomb WHERE id <= 40;
VACUUM tomb;
SELECT count(*) AS c_after FROM tomb WHERE d @@@ 'alpha'::ftsquery;          -- 60
SELECT fts_count('tomb_bm25','alpha'::ftsquery) AS fc_after;                 -- 60
-- delete-all + vacuum + reuse: the reused slots must NOT match 'alpha'
DELETE FROM tomb;
VACUUM tomb;
INSERT INTO tomb SELECT g, to_ftsdoc('beta doc'||g) FROM generate_series(1,60) g;
VACUUM tomb;
SELECT count(*) AS alpha_reused FROM tomb WHERE d @@@ 'alpha'::ftsquery;     -- 0
SELECT count(*) AS beta_reused FROM tomb WHERE d @@@ 'beta'::ftsquery;       -- 60
SELECT fts_count('tomb_bm25','beta'::ftsquery) AS beta_reused_fc;            -- 60
RESET enable_seqscan;
DROP TABLE tomb;

-- oversized document: an analyzed ftsdoc larger than one pending page must be
-- indexed as its own segment rather than rejected
CREATE TABLE bigdoc (id int, d ftsdoc);
CREATE INDEX bigdoc_bm25 ON bigdoc USING bm25 (d);
INSERT INTO bigdoc SELECT 1, to_ftsdoc(string_agg('term'||g||'x', ' '))
  FROM generate_series(1,4000) g;
INSERT INTO bigdoc VALUES (2, to_ftsdoc('small doc with term500x here'));
SET enable_seqscan=off;
SELECT count(*) AS big_term500 FROM bigdoc WHERE d @@@ 'term500x'::ftsquery;   -- 2
SELECT count(*) AS big_term3999 FROM bigdoc WHERE d @@@ 'term3999x'::ftsquery; -- 1
RESET enable_seqscan;
DROP TABLE bigdoc;

-- parallel index build (amcanbuildparallel): a parallel-built index must return
-- exactly what a serial build does.  Force workers with a zero scan threshold.
CREATE TABLE pbuild (id int, d ftsdoc);
INSERT INTO pbuild SELECT g, to_ftsdoc('common term'||(g%200)||' doc'||g)
  FROM generate_series(1,20000) g;
SET min_parallel_table_scan_size = 0;
SET max_parallel_maintenance_workers = 2;
CREATE INDEX pbuild_bm25 ON pbuild USING bm25 (d);
RESET max_parallel_maintenance_workers;
RESET min_parallel_table_scan_size;
SET enable_seqscan = off;
SELECT fts_count('pbuild_bm25', 'common'::ftsquery) AS all_docs;      -- 20000
SELECT fts_count('pbuild_bm25', 'term7'::ftsquery) AS term7;          -- 100
SELECT count(*) AS ranked FROM (SELECT id FROM pbuild WHERE d @@@ 'common'::ftsquery
  ORDER BY d <=> 'common'::ftsquery LIMIT 10) x;                      -- 10
RESET enable_seqscan;
DROP TABLE pbuild;

-- fts_vacuum (1.20): full compaction + tail truncation.  A parallel build
-- leaves dead source-segment pages; fts_vacuum reclaims them and truncates the
-- file, and the contents are unchanged afterward.
ALTER EXTENSION pg_fts UPDATE TO '1.20';
CREATE TABLE vac (id int, d ftsdoc);
INSERT INTO vac SELECT g, to_ftsdoc('common term'||(g%200)||' doc'||g)
  FROM generate_series(1,20000) g;
SET min_parallel_table_scan_size = 0;
SET max_parallel_maintenance_workers = 2;
CREATE INDEX vac_bm25 ON vac USING bm25 (d);
RESET max_parallel_maintenance_workers;
RESET min_parallel_table_scan_size;
SET enable_seqscan = off;
SELECT fts_count('vac_bm25', 'common'::ftsquery) AS before_all;       -- 20000
SELECT fts_vacuum('vac_bm25') IS NOT NULL AS vacuumed;               -- t
SELECT fts_count('vac_bm25', 'common'::ftsquery) AS after_all;        -- 20000
SELECT fts_count('vac_bm25', 'term7'::ftsquery) AS after_term7;       -- 100
SELECT fts_index_nsegments('vac_bm25') AS nseg_after;                 -- 1
RESET enable_seqscan;
DROP TABLE vac;

-- COUNT pushdown CustomScan (transparent count(*) WHERE @@@ answered from the
-- index).  The plan is a Custom Scan (FtsCount); the count matches fts_count;
-- and it must NOT trigger when the shape is unsupported (extra qual, GROUP BY).
CREATE TABLE cnt (id int, body text);
INSERT INTO cnt SELECT g, 'common '||CASE WHEN g%4=0 THEN 'quarter ' ELSE '' END||'w'||(g%100)
  FROM generate_series(1,10000) g;
CREATE INDEX cnt_bm25 ON cnt USING bm25(to_ftsdoc('english',body));
ANALYZE cnt;
SET enable_seqscan=off;
SELECT count(*) = fts_count('cnt_bm25', to_ftsquery('english','common')) AS count_matches
  FROM cnt WHERE to_ftsdoc('english',body) @@@ to_ftsquery('english','common');
SELECT count(*) AS quarter_cnt FROM cnt WHERE to_ftsdoc('english',body) @@@ to_ftsquery('english','quarter');  -- 2500
-- the bare count(*) plan is a Custom Scan (FtsCount)
EXPLAIN (COSTS OFF) SELECT count(*) FROM cnt
  WHERE to_ftsdoc('english',body) @@@ to_ftsquery('english','common');
-- an extra qual disables the pushdown (falls back to Aggregate over a scan)
EXPLAIN (COSTS OFF) SELECT count(*) FROM cnt
  WHERE to_ftsdoc('english',body) @@@ to_ftsquery('english','common') AND id > 5;
RESET enable_seqscan;
DROP TABLE cnt;

