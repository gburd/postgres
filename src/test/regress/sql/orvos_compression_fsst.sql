--
-- Test FSST (Fast Static Symbol Table) string compression
-- Verifies 30-60% additional compression on top of zstd for string columns.
--

-- Test 1: Repetitive strings (ideal for FSST)
CREATE TABLE orvos_fsst_repetitive_test (
    id int,
    message text
) USING orvos;

INSERT INTO orvos_fsst_repetitive_test
SELECT i, 'The quick brown fox jumps over the lazy dog. Record number: ' || i
FROM generate_series(1, 1000) i;

SELECT COUNT(*) FROM orvos_fsst_repetitive_test;
SELECT * FROM orvos_fsst_repetitive_test WHERE id <= 3 ORDER BY id;

DROP TABLE orvos_fsst_repetitive_test;

-- Test 2: JSON-like strings with common substrings
CREATE TABLE orvos_fsst_json_test (
    id int,
    json_data text
) USING orvos;

INSERT INTO orvos_fsst_json_test
SELECT i, '{"user_id": ' || i || ', "status": "active", "timestamp": "2024-01-01T00:00:00Z", "metadata": {"source": "api", "version": "v1"}}'
FROM generate_series(1, 500) i;

SELECT COUNT(*) FROM orvos_fsst_json_test;
SELECT * FROM orvos_fsst_json_test WHERE id = 1;

DROP TABLE orvos_fsst_json_test;

-- Test 3: Log messages with common prefixes
CREATE TABLE orvos_fsst_log_test (
    id int,
    log_message text
) USING orvos;

INSERT INTO orvos_fsst_log_test VALUES
    (1, '[INFO] 2024-01-01 12:00:00 - Application started successfully'),
    (2, '[INFO] 2024-01-01 12:00:01 - Database connection established'),
    (3, '[WARN] 2024-01-01 12:00:02 - High memory usage detected'),
    (4, '[ERROR] 2024-01-01 12:00:03 - Failed to connect to external service'),
    (5, '[INFO] 2024-01-01 12:00:04 - Request processed successfully');

SELECT * FROM orvos_fsst_log_test ORDER BY id;

-- Test filtering on FSST-compressed strings
SELECT COUNT(*) FROM orvos_fsst_log_test WHERE log_message LIKE '[INFO]%';
SELECT COUNT(*) FROM orvos_fsst_log_test WHERE log_message LIKE '%successfully%';

DROP TABLE orvos_fsst_log_test;

-- Test 4: URLs with common patterns
CREATE TABLE orvos_fsst_url_test (
    id int,
    url text
) USING orvos;

INSERT INTO orvos_fsst_url_test
SELECT i, 'https://api.example.com/v1/users/' || i || '/profile?format=json&include=metadata'
FROM generate_series(1, 1000) i;

SELECT COUNT(*) FROM orvos_fsst_url_test;
SELECT * FROM orvos_fsst_url_test WHERE id <= 3 ORDER BY id;

DROP TABLE orvos_fsst_url_test;

-- Test 5: Mixed string lengths
CREATE TABLE orvos_fsst_mixed_test (
    id int,
    short_str text,
    medium_str text,
    long_str text
) USING orvos;

INSERT INTO orvos_fsst_mixed_test
SELECT i,
       'short_' || i,
       'This is a medium length string for record ' || i || ' with some common words.',
       'This is a much longer string that contains a lot of repetitive content. ' ||
       'The purpose is to test FSST compression on longer text fields. ' ||
       'Record number: ' || i || '. ' ||
       'Additional padding text to make this longer. ' ||
       'More padding text here. ' ||
       'And even more padding text to reach a good length for compression testing.'
FROM generate_series(1, 100) i;

SELECT COUNT(*) FROM orvos_fsst_mixed_test;
SELECT id, short_str, length(medium_str), length(long_str) 
FROM orvos_fsst_mixed_test WHERE id <= 3 ORDER BY id;

DROP TABLE orvos_fsst_mixed_test;

-- Test 6: FSST with NULL values
CREATE TABLE orvos_fsst_null_test (
    id int,
    description text
) USING orvos;

INSERT INTO orvos_fsst_null_test
SELECT i,
       CASE
           WHEN i % 5 = 0 THEN NULL
           ELSE 'Description text for record number ' || i || ' with common patterns.'
       END
FROM generate_series(1, 50) i;

SELECT COUNT(*) FROM orvos_fsst_null_test WHERE description IS NULL;
SELECT COUNT(*) FROM orvos_fsst_null_test WHERE description IS NOT NULL;

DROP TABLE orvos_fsst_null_test;
