--
-- Main script, to run all the tests, and print the results.
--
--

-- First run the tests using heap.
DROP SCHEMA IF EXISTS storagetest_heap CASCADE;
CREATE SCHEMA storagetest_heap;
SET search_path='storagetest_heap';

CREATE TABLE results (testname text, val numeric) USING heap;

SET default_table_access_method=heap;
\i tests.sql


-- Repeat with orvos

DROP SCHEMA IF EXISTS storagetest_orvos CASCADE;
CREATE SCHEMA storagetest_orvos;
SET search_path='storagetest_orvos';

CREATE TABLE results (testname text, val numeric) USING heap;

SET default_table_access_method=orvos;
\i tests.sql


SET search_path='public';

SELECT COALESCE(h.testname, zs.testname) as testname,
       h.val as heap,
       zs.val as orvos,
       round(zs.val / h.val, 2) as "heap / orvos"
FROM storagetest_heap.results h
FULL OUTER JOIN storagetest_orvos.results zs ON (h.testname = zs.testname);
