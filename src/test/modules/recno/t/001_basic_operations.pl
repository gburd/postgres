# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# Verify basic RECNO table CRUD operations, VACUUM, ANALYZE, and data integrity.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Utils;
use Test::More;
use PostgreSQL::Test::Cluster;

# Initialize node
my $node = PostgreSQL::Test::Cluster->new('recno_basic');
$node->init;
$node->start;

# ============================================================
# Test 1: Table creation with RECNO access method
# ============================================================

$node->safe_psql('postgres',
	'CREATE TABLE recno_test (id int PRIMARY KEY, val text, data int) USING recno');

my $am = $node->safe_psql('postgres',
	"SELECT amname FROM pg_am a JOIN pg_class c ON c.relam = a.oid
	 WHERE c.relname = 'recno_test'");
is($am, 'recno', "Table created with RECNO access method");

# ============================================================
# Test 2: INSERT operations
# ============================================================

$node->safe_psql('postgres',
	"INSERT INTO recno_test VALUES (1, 'row1', 100)");
$node->safe_psql('postgres',
	"INSERT INTO recno_test VALUES (2, 'row2', 200)");
$node->safe_psql('postgres',
	"INSERT INTO recno_test SELECT i, 'row' || i, i * 100
	 FROM generate_series(3, 100) i");

my $count = $node->safe_psql('postgres', 'SELECT COUNT(*) FROM recno_test');
is($count, '100', "100 rows inserted successfully");

# ============================================================
# Test 3: SELECT operations
# ============================================================

my $val = $node->safe_psql('postgres',
	'SELECT val FROM recno_test WHERE id = 1');
is($val, 'row1', "Point SELECT returns correct value");

my $range = $node->safe_psql('postgres',
	'SELECT COUNT(*) FROM recno_test WHERE id BETWEEN 10 AND 20');
is($range, '11', "Range SELECT returns correct count");

# Verify sequential scan sees all rows
my $seq_count = $node->safe_psql('postgres',
	'SELECT COUNT(*) FROM recno_test');
is($seq_count, '100', "Sequential scan sees all rows");

# ============================================================
# Test 4: UPDATE operations
# ============================================================

# In-place update (same-size value)
$node->safe_psql('postgres',
	'UPDATE recno_test SET data = data + 50 WHERE id <= 10');
my $updated = $node->safe_psql('postgres',
	'SELECT data FROM recno_test WHERE id = 1');
is($updated, '150', "In-place UPDATE works correctly");

# Update text column (variable-length)
$node->safe_psql('postgres',
	"UPDATE recno_test SET val = 'updated_row1' WHERE id = 1");
my $updated_text = $node->safe_psql('postgres',
	'SELECT val FROM recno_test WHERE id = 1');
is($updated_text, 'updated_row1', "Variable-length UPDATE works correctly");

# Verify un-updated rows remain intact
my $intact = $node->safe_psql('postgres',
	'SELECT val FROM recno_test WHERE id = 50');
is($intact, 'row50', "Non-updated rows remain intact");

# ============================================================
# Test 5: DELETE operations
# ============================================================

$node->safe_psql('postgres', 'DELETE FROM recno_test WHERE id > 90');
$count = $node->safe_psql('postgres', 'SELECT COUNT(*) FROM recno_test');
is($count, '90', "DELETE removes rows correctly");

# Verify deleted rows are gone
my $gone = $node->safe_psql('postgres',
	'SELECT COUNT(*) FROM recno_test WHERE id = 95');
is($gone, '0', "Deleted row not visible");

# Verify surviving rows still correct
my $survivor = $node->safe_psql('postgres',
	'SELECT val FROM recno_test WHERE id = 5');
is($survivor, 'row5', "Surviving rows retain correct values after DELETE");

# ============================================================
# Test 6: Index creation and use
# ============================================================

$node->safe_psql('postgres',
	'CREATE INDEX idx_val ON recno_test(val)');
$node->safe_psql('postgres',
	'CREATE INDEX idx_data ON recno_test(data)');

my $indexed = $node->safe_psql('postgres',
	"SELECT id FROM recno_test WHERE val = 'row5'");
is($indexed, '5', "B-tree index scan works on RECNO table");

$count = $node->safe_psql('postgres',
	'SELECT COUNT(*) FROM recno_test WHERE data > 5000');
cmp_ok($count, '>', '0', "Index range scan returns results");

# ============================================================
# Test 7: VACUUM
# ============================================================

$node->safe_psql('postgres', 'VACUUM recno_test');
$count = $node->safe_psql('postgres', 'SELECT COUNT(*) FROM recno_test');
is($count, '90', "VACUUM does not lose data");

# Verify data integrity after VACUUM
my $sum_after_vacuum = $node->safe_psql('postgres',
	'SELECT SUM(id) FROM recno_test');
cmp_ok($sum_after_vacuum, '>', '0', "Data intact after VACUUM");

# ============================================================
# Test 8: ANALYZE
# ============================================================

$node->safe_psql('postgres', 'ANALYZE recno_test');
my $stats = $node->safe_psql('postgres',
	"SELECT n_live_tup FROM pg_stat_user_tables WHERE relname = 'recno_test'");
cmp_ok($stats, '>', '0', "ANALYZE collects statistics");

# ============================================================
# Test 9: VACUUM FULL
# ============================================================

# Record data checksum before VACUUM FULL
my $checksum_before = $node->safe_psql('postgres',
	'SELECT SUM(id), SUM(data), COUNT(*) FROM recno_test');

$node->safe_psql('postgres', 'VACUUM FULL recno_test');

my $checksum_after = $node->safe_psql('postgres',
	'SELECT SUM(id), SUM(data), COUNT(*) FROM recno_test');
is($checksum_after, $checksum_before, "VACUUM FULL preserves all data");

# ============================================================
# Test 10: Multi-page operations (large inserts)
# ============================================================

$node->safe_psql('postgres',
	"INSERT INTO recno_test SELECT i, repeat('x', 100), i
	 FROM generate_series(101, 1000) i");
$count = $node->safe_psql('postgres', 'SELECT COUNT(*) FROM recno_test');
is($count, '990', "Multi-page inserts work correctly");

# Verify data integrity across pages
my $last_val = $node->safe_psql('postgres',
	'SELECT data FROM recno_test WHERE id = 1000');
is($last_val, '1000', "Data correct across multiple pages");

# ============================================================
# Test 11: Transactional integrity
# ============================================================

# Committed transaction
$node->safe_psql('postgres',
	"BEGIN;
	 INSERT INTO recno_test VALUES (1001, 'txn_committed', 9999);
	 COMMIT");
my $committed = $node->safe_psql('postgres',
	'SELECT val FROM recno_test WHERE id = 1001');
is($committed, 'txn_committed', "Committed transaction visible");

# Rolled-back transaction
$node->safe_psql('postgres',
	"BEGIN;
	 INSERT INTO recno_test VALUES (1002, 'txn_rolledback', 8888);
	 ROLLBACK");
my $rolledback = $node->safe_psql('postgres',
	'SELECT COUNT(*) FROM recno_test WHERE id = 1002');
is($rolledback, '0', "Rolled-back transaction not visible");

# ============================================================
# Test 12: VACUUM ANALYZE after mixed operations
# ============================================================

$node->safe_psql('postgres', 'VACUUM ANALYZE recno_test');
$count = $node->safe_psql('postgres', 'SELECT COUNT(*) FROM recno_test');
is($count, '991', "VACUUM ANALYZE preserves data after mixed operations");

# Verify index still works after VACUUM ANALYZE
my $plan = $node->safe_psql('postgres',
	"EXPLAIN (COSTS OFF) SELECT val FROM recno_test WHERE val = 'row5'");
like($plan, qr/Index/, "Index scan still works after VACUUM ANALYZE");

# ============================================================
# Test 13: TRUNCATE
# ============================================================

$node->safe_psql('postgres',
	'CREATE TABLE recno_truncate_test (id int, val text) USING recno');
$node->safe_psql('postgres',
	"INSERT INTO recno_truncate_test SELECT i, 'val' || i
	 FROM generate_series(1, 100) i");
$node->safe_psql('postgres', 'TRUNCATE recno_truncate_test');
$count = $node->safe_psql('postgres',
	'SELECT COUNT(*) FROM recno_truncate_test');
is($count, '0', "TRUNCATE removes all rows");

# Can insert again after truncate
$node->safe_psql('postgres',
	"INSERT INTO recno_truncate_test VALUES (1, 'after_truncate')");
$val = $node->safe_psql('postgres',
	'SELECT val FROM recno_truncate_test WHERE id = 1');
is($val, 'after_truncate', "INSERT works after TRUNCATE");

$node->stop;

done_testing();
