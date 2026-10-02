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

-- Duplicate keys form LIST entries (sorted locator lists); the verifier checks
-- that each list has >= 2 members stored strictly ascending.  Insert the
-- duplicates (the insert path coalesces equal keys into LISTs) and verify, then
-- shrink some lists by vacuuming away part of a key's rows and verify again.
CREATE TABLE bark_check_list (a int, b int);
CREATE INDEX bark_check_list_idx ON bark_check_list USING bark (a);
INSERT INTO bark_check_list SELECT g % 50, g FROM generate_series(1, 10000) g;
SELECT bark_index_check('bark_check_list_idx');
DELETE FROM bark_check_list WHERE a = 10 AND b < 5000;
VACUUM bark_check_list;
SELECT bark_index_check('bark_check_list_idx');
DROP TABLE bark_check_list;

DROP TABLE bark_check_tab;
