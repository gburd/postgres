-- Comprehensive tests for External BLOB/CLOB with UNDO integration
-- Tests: creation, deduplication, delta updates, compaction,
--        transaction rollback, CLOB text operations, encoding

-- ============================================================
-- Setup
-- ============================================================
CREATE TABLE eb_blob_test (
    id serial PRIMARY KEY,
    tag text,
    data blob
);

CREATE TABLE eb_clob_test (
    id serial PRIMARY KEY,
    tag text,
    content clob
);

-- ============================================================
-- Test 1: BLOB creation and retrieval
-- ============================================================
SELECT 'Test 1: BLOB creation' AS test;

INSERT INTO eb_blob_test (tag, data) VALUES
    ('hello', '\x48656C6C6F'::blob);

SELECT tag, data FROM eb_blob_test WHERE tag = 'hello';

-- ============================================================
-- Test 2: CLOB creation and retrieval
-- ============================================================
SELECT 'Test 2: CLOB creation' AS test;

INSERT INTO eb_clob_test (tag, content) VALUES
    ('greeting', 'Hello, World!');

SELECT tag, content::text FROM eb_clob_test WHERE tag = 'greeting';

-- ============================================================
-- Test 3: Content-addressable deduplication
-- ============================================================
SELECT 'Test 3: Deduplication' AS test;

-- Insert same content four times
INSERT INTO eb_blob_test (tag, data) VALUES
    ('dup_a', '\xDEADBEEF'::blob),
    ('dup_b', '\xDEADBEEF'::blob),
    ('dup_c', '\xDEADBEEF'::blob),
    ('dup_d', '\xDEADBEEF'::blob);

-- All refs should be equal (same hash, same version)
SELECT COUNT(*) AS total FROM eb_blob_test WHERE tag LIKE 'dup_%';
SELECT COUNT(DISTINCT data) AS distinct_values FROM eb_blob_test WHERE tag LIKE 'dup_%';

-- ============================================================
-- Test 4: Delta updates on substantial content
-- ============================================================
SELECT 'Test 4: Delta updates' AS test;

-- Create a 4KB blob (above blob_delta_threshold)
INSERT INTO eb_blob_test (tag, data) VALUES
    ('delta_src', decode(repeat('41424344', 1024), 'hex')::blob);

SELECT tag, octet_length(data::bytea) AS size
FROM eb_blob_test WHERE tag = 'delta_src';

-- Update with minor change (last 4 bytes differ) -- should produce a delta
UPDATE eb_blob_test
SET data = decode(repeat('41424344', 1023) || '45464748', 'hex')::blob
WHERE tag = 'delta_src';

SELECT tag, octet_length(data::bytea) AS size
FROM eb_blob_test WHERE tag = 'delta_src';

-- ============================================================
-- Test 5: Multiple sequential updates (delta chain)
-- ============================================================
SELECT 'Test 5: Delta chain' AS test;

INSERT INTO eb_blob_test (tag, data) VALUES
    ('chain', decode(repeat('AA', 2048), 'hex')::blob);

-- Apply several small updates to build a delta chain
UPDATE eb_blob_test SET data = decode('BB' || repeat('AA', 2047), 'hex')::blob WHERE tag = 'chain';
UPDATE eb_blob_test SET data = decode('BBCC' || repeat('AA', 2046), 'hex')::blob WHERE tag = 'chain';
UPDATE eb_blob_test SET data = decode('BBCCDD' || repeat('AA', 2045), 'hex')::blob WHERE tag = 'chain';

SELECT tag, octet_length(data::bytea) AS size
FROM eb_blob_test WHERE tag = 'chain';

-- ============================================================
-- Test 6: Transaction rollback cleans up blob files
-- ============================================================
SELECT 'Test 6: Transaction rollback' AS test;

BEGIN;
INSERT INTO eb_blob_test (tag, data) VALUES
    ('rollback_me', '\xCAFEBABE01020304'::blob);
SELECT COUNT(*) AS during_txn FROM eb_blob_test WHERE tag = 'rollback_me';
ROLLBACK;

SELECT COUNT(*) AS after_rollback FROM eb_blob_test WHERE tag = 'rollback_me';

-- ============================================================
-- Test 7: Transaction commit persists blob
-- ============================================================
SELECT 'Test 7: Transaction commit' AS test;

BEGIN;
INSERT INTO eb_blob_test (tag, data) VALUES
    ('committed', '\xCAFEBABE05060708'::blob);
COMMIT;

SELECT COUNT(*) AS after_commit FROM eb_blob_test WHERE tag = 'committed';
SELECT tag, data FROM eb_blob_test WHERE tag = 'committed';

-- ============================================================
-- Test 8: CLOB text operations (external_clob.c functions)
-- ============================================================
SELECT 'Test 8: CLOB text operations' AS test;

INSERT INTO eb_clob_test (tag, content) VALUES
    ('ops_test', 'The quick brown fox jumps over the lazy dog');

-- Character length
SELECT tag, clob_length(content) AS char_len
FROM eb_clob_test WHERE tag = 'ops_test';

-- Byte length
SELECT tag, clob_octet_length(content) AS byte_len
FROM eb_clob_test WHERE tag = 'ops_test';

-- Substring extraction (1-based, 10 chars starting at position 5)
SELECT tag, clob_substring(content, 5, 10) AS substr
FROM eb_clob_test WHERE tag = 'ops_test';

-- Encoding name
SELECT tag, clob_encoding(content) AS encoding
FROM eb_clob_test WHERE tag = 'ops_test';

-- ============================================================
-- Test 9: CLOB concatenation
-- ============================================================
SELECT 'Test 9: CLOB concatenation' AS test;

INSERT INTO eb_clob_test (tag, content) VALUES
    ('concat_a', 'Hello, '),
    ('concat_b', 'World!');

SELECT clob_concat(a.content, b.content)::text AS concatenated
FROM eb_clob_test a, eb_clob_test b
WHERE a.tag = 'concat_a' AND b.tag = 'concat_b';

-- ============================================================
-- Test 10: CLOB LIKE pattern matching
-- ============================================================
SELECT 'Test 10: CLOB LIKE' AS test;

SELECT tag, clob_like(content, '%quick%') AS matches_quick,
       clob_like(content, '%slow%') AS matches_slow
FROM eb_clob_test WHERE tag = 'ops_test';

-- ============================================================
-- Test 11: Large CLOB (repeated text)
-- ============================================================
SELECT 'Test 11: Large CLOB' AS test;

INSERT INTO eb_clob_test (tag, content) VALUES
    ('large_text', repeat('Lorem ipsum dolor sit amet. ', 200));

SELECT tag, clob_length(content) AS char_len,
       clob_octet_length(content) AS byte_len
FROM eb_clob_test WHERE tag = 'large_text';

-- ============================================================
-- Test 12: CLOB deduplication
-- ============================================================
SELECT 'Test 12: CLOB deduplication' AS test;

INSERT INTO eb_clob_test (tag, content) VALUES
    ('clob_dup1', 'identical text content'),
    ('clob_dup2', 'identical text content'),
    ('clob_dup3', 'identical text content');

SELECT COUNT(*) AS total FROM eb_clob_test WHERE tag LIKE 'clob_dup%';
SELECT COUNT(DISTINCT content) AS distinct_values FROM eb_clob_test WHERE tag LIKE 'clob_dup%';

-- ============================================================
-- Test 13: NULL blob and clob handling
-- ============================================================
SELECT 'Test 13: NULL handling' AS test;

INSERT INTO eb_blob_test (tag, data) VALUES ('null_data', NULL);
INSERT INTO eb_clob_test (tag, content) VALUES ('null_content', NULL);

SELECT tag, data IS NULL AS is_null FROM eb_blob_test WHERE tag = 'null_data';
SELECT tag, content IS NULL AS is_null FROM eb_clob_test WHERE tag = 'null_content';

-- ============================================================
-- Test 14: Blob comparison operators
-- ============================================================
SELECT 'Test 14: Comparison operators' AS test;

INSERT INTO eb_blob_test (tag, data) VALUES
    ('cmp_a', '\x0001'::blob),
    ('cmp_b', '\x0002'::blob),
    ('cmp_c', '\x0001'::blob);

SELECT a.tag AS tag_a, b.tag AS tag_b, (a.data = b.data) AS eq
FROM eb_blob_test a, eb_blob_test b
WHERE a.tag = 'cmp_a' AND b.tag = 'cmp_c';

SELECT a.tag AS tag_a, b.tag AS tag_b, (a.data < b.data) AS lt
FROM eb_blob_test a, eb_blob_test b
WHERE a.tag = 'cmp_a' AND b.tag = 'cmp_b';

-- ============================================================
-- Test 15: Empty blob and clob
-- ============================================================
SELECT 'Test 15: Empty values' AS test;

INSERT INTO eb_blob_test (tag, data) VALUES ('empty_blob', '\x'::blob);
INSERT INTO eb_clob_test (tag, content) VALUES ('empty_clob', '');

SELECT tag, octet_length(data::bytea) AS size FROM eb_blob_test WHERE tag = 'empty_blob';
SELECT tag, clob_length(content) AS char_len FROM eb_clob_test WHERE tag = 'empty_clob';

-- ============================================================
-- Test 16: Deletion and row count verification
-- ============================================================
SELECT 'Test 16: Deletion' AS test;

SELECT COUNT(*) AS before_delete FROM eb_blob_test;

DELETE FROM eb_blob_test WHERE tag LIKE 'dup_%';

SELECT COUNT(*) AS after_delete FROM eb_blob_test;

-- ============================================================
-- Cleanup
-- ============================================================
DROP TABLE eb_blob_test CASCADE;
DROP TABLE eb_clob_test CASCADE;

SELECT 'All external BLOB/CLOB tests passed' AS result;
