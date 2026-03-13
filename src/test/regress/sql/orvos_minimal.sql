-- Minimal delta UPDATE test to see ORVOS debug output
CREATE TABLE test_chain(a int, b int, c text) USING orvos;
INSERT INTO test_chain VALUES (1, 10, 'hello');
UPDATE test_chain SET b = 20 WHERE a = 1;
UPDATE test_chain SET b = 30 WHERE a = 1;
SELECT * FROM test_chain WHERE a = 1;
DROP TABLE test_chain;
