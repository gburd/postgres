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
