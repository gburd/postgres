-- pgbench custom script: TPC-B-like workload on RECNO tables
-- Simulates mixed read-write OLTP workload
\set aid random(1, 100000)
\set bid random(1, 10)
\set tid random(1, 100)
\set delta random(-5000, 5000)
BEGIN;
UPDATE pgbench_recno_accounts SET abalance = abalance + :delta WHERE aid = :aid;
SELECT abalance FROM pgbench_recno_accounts WHERE aid = :aid;
UPDATE pgbench_recno_tellers SET tbalance = tbalance + :delta WHERE tid = :tid;
UPDATE pgbench_recno_branches SET bbalance = bbalance + :delta WHERE bid = :bid;
COMMIT;
