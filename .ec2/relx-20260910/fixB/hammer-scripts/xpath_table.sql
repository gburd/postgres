-- Fix B concurrency hammer: xpath_table, well-formed rows only for the sustained TPS run
SELECT count(*) FROM xpath_table('id', 'xmldoc', 'hammer_docs', '/doc/int', 'id <= 500') AS t(id int4, val int4);
