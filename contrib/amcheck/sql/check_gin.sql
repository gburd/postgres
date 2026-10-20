-- Test of index bulk load
SELECT setseed(1);
CREATE TABLE "gin_check"("Column1" int[]);
-- posting trees (frequently used entries)
INSERT INTO gin_check select array_agg(round(random()*255) ) from generate_series(1, 100000) as i group by i % 10000;
-- posting leaves (sparse entries)
INSERT INTO gin_check select array_agg(255 + round(random()*100)) from generate_series(1, 100) as i group by i % 100;
CREATE INDEX gin_check_idx on "gin_check" USING GIN("Column1");
SELECT gin_index_check('gin_check_idx');

-- cleanup
DROP TABLE gin_check;

-- Test index inserts
SELECT setseed(1);
CREATE TABLE "gin_check"("Column1" int[]);
CREATE INDEX gin_check_idx on "gin_check" USING GIN("Column1");
ALTER INDEX gin_check_idx SET (fastupdate = false);
-- posting trees
INSERT INTO gin_check select array_agg(round(random()*255) ) from generate_series(1, 100000) as i group by i % 10000;
-- posting leaves
INSERT INTO gin_check select array_agg(100 + round(random()*255)) from generate_series(1, 100) as i group by i % 100;

SELECT gin_index_check('gin_check_idx');

-- cleanup
DROP TABLE gin_check;

-- Test GIN over text array
SELECT setseed(1);
CREATE TABLE "gin_check_text_array"("Column1" text[]);
-- posting trees
INSERT INTO gin_check_text_array select array_agg(sha256(round(random()*300)::text::bytea)::text) from generate_series(1, 100000) as i group by i % 10000;
-- posting leaves
INSERT INTO gin_check_text_array select array_agg(sha256(round(random()*300 + 300)::text::bytea)::text) from generate_series(1, 10000) as i group by i % 100;
CREATE INDEX gin_check_text_array_idx on "gin_check_text_array" USING GIN("Column1");
SELECT gin_index_check('gin_check_text_array_idx');

-- cleanup
DROP TABLE gin_check_text_array;

-- Test GIN over jsonb
CREATE TABLE "gin_check_jsonb"("j" jsonb);
INSERT INTO gin_check_jsonb values ('{"a":[["b",{"x":1}],["b",{"x":2}]],"c":3}');
INSERT INTO gin_check_jsonb values ('[[14,2,3]]');
INSERT INTO gin_check_jsonb values ('[1,[14,2,3]]');
CREATE INDEX "gin_check_jsonb_idx" on gin_check_jsonb USING GIN("j" jsonb_path_ops);

SELECT gin_index_check('gin_check_jsonb_idx');

-- cleanup
DROP TABLE gin_check_jsonb;

-- Test GIN multicolumn index
CREATE TABLE "gin_check_multicolumn"(a text[], b text[]);
INSERT INTO gin_check_multicolumn (a,b) values ('{a,c,e}','{b,d,f}');
CREATE INDEX "gin_check_multicolumn_idx" on gin_check_multicolumn USING GIN(a,b);

SELECT gin_index_check('gin_check_multicolumn_idx');

-- cleanup
DROP TABLE gin_check_multicolumn;

-- An UPDATE that leaves a GIN index's keys for the row as they are is HOT:
-- reordering or repeating array elements, or changing the positions in a
-- tsvector but not its lexemes.  Scans through the index must still find
-- exactly the rows a sequential scan finds.
CREATE TABLE gin_hot (id int, tags int[], tsv tsvector, body text)
  WITH (fillfactor = 10);
CREATE INDEX gin_hot_tags ON gin_hot USING gin (tags);
CREATE INDEX gin_hot_tsv ON gin_hot USING gin (tsv);
CREATE INDEX gin_hot_body ON gin_hot USING gin (to_tsvector('simple', body));
INSERT INTO gin_hot
  SELECT g, ARRAY[g % 7, g % 11, g % 13], to_tsvector('simple', 'a b c ' || g),
         'one two ' || g
    FROM generate_series(1, 100) g;
CREATE FUNCTION gin_hot_count(OUT updated bigint, OUT hot bigint)
LANGUAGE sql AS $$
  SELECT pg_stat_get_xact_tuples_updated('gin_hot'::regclass),
         pg_stat_get_xact_tuples_hot_updated('gin_hot'::regclass) $$;
-- How many rows each tag, lexeme and word matches, through the index and
-- not, and the number of mismatches.
CREATE FUNCTION gin_hot_mismatches() RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE n bigint := 0; k int; w text; a bigint; b bigint;
BEGIN
  FOR k IN 0..13 LOOP
    SET LOCAL enable_seqscan = off; SET LOCAL enable_indexscan = off;
    SELECT count(*) INTO a FROM gin_hot WHERE tags @> ARRAY[k];
    SET LOCAL enable_seqscan = on; SET LOCAL enable_bitmapscan = off;
    SELECT count(*) INTO b FROM gin_hot WHERE tags @> ARRAY[k];
    RESET enable_bitmapscan; RESET enable_indexscan;
    IF a <> b THEN n := n + 1; END IF;
  END LOOP;
  FOREACH w IN ARRAY ARRAY['a', 'b', 'c', 'd', 'one', 'two', 'three', '7']
  LOOP
    SET LOCAL enable_seqscan = off; SET LOCAL enable_indexscan = off;
    SELECT count(*) INTO a FROM gin_hot WHERE tsv @@ to_tsquery('simple', w);
    SELECT count(*) INTO b FROM gin_hot
      WHERE to_tsvector('simple', body) @@ to_tsquery('simple', w);
    SET LOCAL enable_seqscan = on; SET LOCAL enable_bitmapscan = off;
    IF a <> (SELECT count(*) FROM gin_hot
             WHERE tsv @@ to_tsquery('simple', w)) THEN n := n + 1; END IF;
    IF b <> (SELECT count(*) FROM gin_hot
             WHERE to_tsvector('simple', body) @@ to_tsquery('simple', w))
      THEN n := n + 1; END IF;
    RESET enable_bitmapscan; RESET enable_indexscan;
  END LOOP;
  RETURN n;
END $$;
BEGIN;
-- same keys: HOT
UPDATE gin_hot SET tags = ARRAY[tags[3], tags[1], tags[2], tags[1]];
SELECT * FROM gin_hot_count();
UPDATE gin_hot SET tsv = to_tsvector('simple', 'c b a a ' || id);
SELECT * FROM gin_hot_count();
UPDATE gin_hot SET body = id || ' two one one';
SELECT * FROM gin_hot_count();
-- different keys: not HOT
UPDATE gin_hot SET tags = tags || 0 WHERE id <= 10;
SELECT * FROM gin_hot_count();
UPDATE gin_hot SET tsv = to_tsvector('simple', 'a b d ' || id) WHERE id <= 10;
SELECT * FROM gin_hot_count();
UPDATE gin_hot SET body = 'three ' || body WHERE id <= 10;
SELECT * FROM gin_hot_count();
-- to and from no keys, and null
UPDATE gin_hot SET tags = '{}' WHERE id = 11;
UPDATE gin_hot SET tags = NULL WHERE id = 11;
UPDATE gin_hot SET tags = '{1}' WHERE id = 11;
SELECT * FROM gin_hot_count();
COMMIT;
SELECT gin_hot_mismatches();
SELECT gin_index_check('gin_hot_tags');
SELECT gin_index_check('gin_hot_tsv');
SELECT gin_index_check('gin_hot_body');
DROP TABLE gin_hot;
DROP FUNCTION gin_hot_count;
DROP FUNCTION gin_hot_mismatches;
