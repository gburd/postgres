-- Fix B error-path concurrency: xpath_table against the FULL table (well-formed + malformed rows).
-- xpath_table's contract: not-well-formed docs yield an all-NULL row, never an error (xpath.c comment).
SELECT count(*) FROM xpath_table('id', 'xmldoc', 'hammer_docs', '/doc/int', 'true') AS t(id int4, val int4);
