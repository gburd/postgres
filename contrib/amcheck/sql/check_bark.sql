-- bark_index_check() verifies the structure of a BARK index.
-- (The amcheck extension is created by the earlier "check" test.)

-- Rejects a non-BARK index.
CREATE TABLE bark_check_tab (a int, b int);
INSERT INTO bark_check_tab SELECT g, g % 100 FROM generate_series(1, 20000) g;
CREATE INDEX bark_check_btree ON bark_check_tab USING btree (a);
SELECT bark_index_check('bark_check_btree');  -- errors: not a BARK index

-- A freshly built multi-level BARK index passes.
CREATE INDEX bark_check_idx ON bark_check_tab USING bark (a);
SELECT bark_index_check('bark_check_idx');

-- Still passes after inserts (page splits), deletes, and a vacuum.
INSERT INTO bark_check_tab SELECT g, g % 100 FROM generate_series(20001, 40000) g;
SELECT bark_index_check('bark_check_idx');
DELETE FROM bark_check_tab WHERE a <= 10000;
VACUUM bark_check_tab;
SELECT bark_index_check('bark_check_idx');

-- A multi-column index with INCLUDE columns passes (pivots are truncated).
CREATE INDEX bark_check_multi
  ON bark_check_tab USING bark (b, a) INCLUDE (a);
SELECT bark_index_check('bark_check_multi');

DROP TABLE bark_check_tab;
