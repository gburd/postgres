--
-- bench_cleanup.sql
--
-- Drops all benchmark tables to free space.
--

-- Compression benchmark tables
DROP TABLE IF EXISTS heap_comp_int CASCADE;
DROP TABLE IF EXISTS recno_comp_int CASCADE;
DROP TABLE IF EXISTS heap_comp_text CASCADE;
DROP TABLE IF EXISTS recno_comp_text CASCADE;
DROP TABLE IF EXISTS heap_comp_repeat CASCADE;
DROP TABLE IF EXISTS recno_comp_repeat CASCADE;
DROP TABLE IF EXISTS heap_comp_numeric CASCADE;
DROP TABLE IF EXISTS recno_comp_numeric CASCADE;
DROP TABLE IF EXISTS heap_comp_entropy CASCADE;
DROP TABLE IF EXISTS recno_comp_entropy CASCADE;
DROP TABLE IF EXISTS heap_comp_mixed CASCADE;
DROP TABLE IF EXISTS recno_comp_mixed CASCADE;

-- Bulk insert benchmark tables
DROP TABLE IF EXISTS heap_bulk_100k CASCADE;
DROP TABLE IF EXISTS recno_bulk_100k CASCADE;
DROP TABLE IF EXISTS heap_bulk_1m CASCADE;
DROP TABLE IF EXISTS recno_bulk_1m CASCADE;
DROP TABLE IF EXISTS heap_bulk_10m CASCADE;
DROP TABLE IF EXISTS recno_bulk_10m CASCADE;

-- Update benchmark tables
DROP TABLE IF EXISTS heap_update_test CASCADE;
DROP TABLE IF EXISTS recno_update_test CASCADE;

-- Scan benchmark tables
DROP TABLE IF EXISTS heap_scan_test CASCADE;
DROP TABLE IF EXISTS recno_scan_test CASCADE;

-- pgbench tables
DROP TABLE IF EXISTS pgbench_heap_accounts CASCADE;
DROP TABLE IF EXISTS pgbench_heap_tellers CASCADE;
DROP TABLE IF EXISTS pgbench_heap_branches CASCADE;
DROP TABLE IF EXISTS pgbench_recno_accounts CASCADE;
DROP TABLE IF EXISTS pgbench_recno_tellers CASCADE;
DROP TABLE IF EXISTS pgbench_recno_branches CASCADE;
