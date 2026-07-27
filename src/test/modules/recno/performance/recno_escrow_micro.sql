-- RECNO escrow micro-benchmark: single hot counter row.
-- Every client hammers the SAME row with a commutative increment.
--
-- escrow-OFF: the escrow reloption is NOT set, so `UPDATE t SET ytd=ytd+1`
--   takes the ordinary RECNO CAS path: the second concurrent writer finds
--   RECNO_TUPLE_UNCOMMITTED and XactLockTableWaits until the first commits.
--   Throughput is expected to peak at low client counts then roll off /
--   decline as commit-length serialization dominates the hot row.
--
-- escrow-ON: the column is flagged escrow=true, so concurrent writers apply
--   their += on the running sum under the content lock (latch-length wait,
--   no XactLockTableWait).  Throughput is expected to hold or climb with
--   client count rather than roll off.
--
-- Driver: pgbench -c N -T <secs> -f recno_escrow_micro.sql
\set delta 1
UPDATE esc_micro SET ytd = ytd + :delta WHERE id = 1;
