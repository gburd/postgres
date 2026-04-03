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


-- Repeat with noxu

DROP SCHEMA IF EXISTS storagetest_noxu CASCADE;
CREATE SCHEMA storagetest_noxu;
SET search_path='storagetest_noxu';

CREATE TABLE results (testname text, val numeric) USING heap;

SET default_table_access_method=noxu;
\i tests.sql


SET search_path='public';

SELECT COALESCE(h.testname, zs.testname) as testname,
       h.val as heap,
       zs.val as noxu,
       round(zs.val / h.val, 2) as "heap / noxu"
FROM storagetest_heap.results h
FULL OUTER JOIN storagetest_noxu.results zs ON (h.testname = zs.testname);
