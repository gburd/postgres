-- Minimal test for predecessor chain debugging
DROP TABLE IF EXISTS test_chain;
CREATE TABLE test_chain(a int, b int, c text) USING orvos;
INSERT INTO test_chain VALUES (1, 10, 'hello');
UPDATE test_chain SET b = 20;
UPDATE test_chain SET b = 30;
SELECT * FROM test_chain;
