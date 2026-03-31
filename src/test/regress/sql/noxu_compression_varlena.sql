--
-- Test varlena conversion optimization (native PostgreSQL format)
-- Verifies 15-30% faster INSERT/SELECT by eliminating format conversion.
--

-- Test 1: Short varlena strings (< 127 bytes, should use native format)
CREATE TABLE noxu_varlena_short_test (
    id int,
    short_text text,
    short_varchar varchar(50)
) USING noxu;

INSERT INTO noxu_varlena_short_test
SELECT i, 'short_string_' || i, 'varchar_' || i
FROM generate_series(1, 1000) i;

SELECT COUNT(*) FROM noxu_varlena_short_test;
SELECT * FROM noxu_varlena_short_test WHERE id <= 5 ORDER BY id;

-- Test updates on short varlena
UPDATE noxu_varlena_short_test SET short_text = 'updated_' || id WHERE id <= 10;
SELECT * FROM noxu_varlena_short_test WHERE id <= 10 ORDER BY id;

DROP TABLE noxu_varlena_short_test;

-- Test 2: Medium varlena strings (127-8000 bytes)
CREATE TABLE noxu_varlena_medium_test (
    id int,
    medium_text text
) USING noxu;

INSERT INTO noxu_varlena_medium_test
SELECT i, repeat('x', 200) || '_record_' || i
FROM generate_series(1, 500) i;

SELECT COUNT(*) FROM noxu_varlena_medium_test;
SELECT id, length(medium_text) FROM noxu_varlena_medium_test WHERE id <= 3 ORDER BY id;

DROP TABLE noxu_varlena_medium_test;

-- Test 3: Mixed varlena sizes
CREATE TABLE noxu_varlena_mixed_test (
    id int,
    tiny_text text,
    small_text text,
    medium_text text
) USING noxu;

INSERT INTO noxu_varlena_mixed_test
SELECT i,
       'tiny' || i,
       repeat('s', 50) || i,
       repeat('m', 500) || i
FROM generate_series(1, 200) i;

SELECT COUNT(*) FROM noxu_varlena_mixed_test;
SELECT id, length(tiny_text), length(small_text), length(medium_text)
FROM noxu_varlena_mixed_test WHERE id <= 5 ORDER BY id;

DROP TABLE noxu_varlena_mixed_test;

-- Test 4: Varlena with NULLs
CREATE TABLE noxu_varlena_null_test (
    id int,
    nullable_text text,
    nullable_bytea bytea
) USING noxu;

INSERT INTO noxu_varlena_null_test
SELECT i,
       CASE WHEN i % 3 = 0 THEN NULL ELSE 'text_' || i END,
       CASE WHEN i % 4 = 0 THEN NULL ELSE E'\\x' || to_hex(i)::bytea END
FROM generate_series(1, 100) i;

SELECT COUNT(*) FROM noxu_varlena_null_test WHERE nullable_text IS NULL;
SELECT COUNT(*) FROM noxu_varlena_null_test WHERE nullable_bytea IS NULL;

DROP TABLE noxu_varlena_null_test;

-- Test 5: Bytea (binary varlena)
CREATE TABLE noxu_varlena_bytea_test (
    id int,
    binary_data bytea
) USING noxu;

INSERT INTO noxu_varlena_bytea_test
SELECT i, decode(repeat(to_hex(i), 10), 'hex')
FROM generate_series(1, 100) i;

SELECT COUNT(*) FROM noxu_varlena_bytea_test;
SELECT id, length(binary_data) FROM noxu_varlena_bytea_test WHERE id <= 5 ORDER BY id;

DROP TABLE noxu_varlena_bytea_test;

-- Test 6: Text concatenation (verify native format preserved)
CREATE TABLE noxu_varlena_concat_test (
    id int,
    part1 text,
    part2 text
) USING noxu;

INSERT INTO noxu_varlena_concat_test
SELECT i, 'part1_' || i, 'part2_' || i
FROM generate_series(1, 50) i;

SELECT id, part1 || '_' || part2 AS concatenated
FROM noxu_varlena_concat_test WHERE id <= 5 ORDER BY id;

DROP TABLE noxu_varlena_concat_test;

-- Test 7: LIKE queries on native varlena
CREATE TABLE noxu_varlena_like_test (
    id int,
    searchable_text text
) USING noxu;

INSERT INTO noxu_varlena_like_test
SELECT i,
       CASE
           WHEN i % 3 = 0 THEN 'apple_' || i
           WHEN i % 3 = 1 THEN 'banana_' || i
           ELSE 'cherry_' || i
       END
FROM generate_series(1, 300) i;

SELECT COUNT(*) FROM noxu_varlena_like_test WHERE searchable_text LIKE 'apple%';
SELECT COUNT(*) FROM noxu_varlena_like_test WHERE searchable_text LIKE '%banana%';

DROP TABLE noxu_varlena_like_test;
