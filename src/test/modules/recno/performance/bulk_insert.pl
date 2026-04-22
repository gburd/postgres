# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# Performance benchmark: Bulk insert throughput for RECNO vs HEAP.
#
# Measures:
#   - Insert throughput (rows/sec) at 1M and 10M row scales
#   - Storage size (bytes and pretty-printed)
#   - Wall-clock time per insert batch
#
# Output: CSV file at performance/results/bulk_insert.csv

use strict;
use warnings FATAL => 'all';

use File::Basename;
use File::Path qw(make_path);
use Time::HiRes qw(gettimeofday tv_interval);
use PostgreSQL::Test::Utils;
use PostgreSQL::Test::Cluster;

# Results directory next to this script
my $script_dir  = dirname(__FILE__);
my $results_dir = "$script_dir/results";
make_path($results_dir) unless -d $results_dir;

my $csv_file = "$results_dir/bulk_insert.csv";

# Initialize PostgreSQL node
my $node = PostgreSQL::Test::Cluster->new('bench_bulk_insert');
$node->init;

# Tune for benchmark workload
$node->append_conf('postgresql.conf', <<'CONF');
shared_buffers = '256MB'
work_mem = '64MB'
maintenance_work_mem = '256MB'
wal_level = minimal
max_wal_senders = 0
fsync = off
synchronous_commit = off
full_page_writes = off
checkpoint_timeout = '30min'
max_wal_size = '4GB'
CONF

$node->start;

# Open CSV output
open(my $csv, '>', $csv_file) or die "Cannot open $csv_file: $!";
print $csv "benchmark,access_method,rows,metric,value,unit\n";

# Helper: run a timed INSERT and return elapsed seconds
sub timed_insert
{
	my ($node, $sql) = @_;
	my $t0 = [gettimeofday];
	$node->safe_psql('postgres', $sql);
	return tv_interval($t0);
}

# Helper: get relation size in bytes
sub relation_size
{
	my ($node, $table) = @_;
	return $node->safe_psql('postgres',
		"SELECT pg_relation_size('$table')");
}

# Helper: get relation size pretty
sub relation_size_pretty
{
	my ($node, $table) = @_;
	return $node->safe_psql('postgres',
		"SELECT pg_size_pretty(pg_relation_size('$table'))");
}

# Helper: emit one CSV row and print to stdout
sub emit
{
	my ($am, $rows, $metric, $value, $unit) = @_;
	print $csv "bulk_insert,$am,$rows,$metric,$value,$unit\n";
	printf "  %-6s %10s rows  %-20s %12s %s\n", $am, $rows, $metric, $value,
	  $unit;
}

print "=" x 60, "\n";
print "Bulk Insert Benchmark: RECNO vs HEAP\n";
print "=" x 60, "\n";

# ======================================================================
# Test configurations: [label, row_count, table_schema]
# ======================================================================
my @scales = (
	[
		'mixed_1M', 1_000_000,
		"(id INT4, val INT8, name TEXT, data BYTEA)",
		"SELECT i, i * 17, 'User-' || i || '-record-' || (i % 1000), decode(md5(i::text), 'hex') FROM generate_series(1, 1000000) i"
	],
	[
		'int_10M', 10_000_000,
		"(id INT4, a INT4, b INT8)",
		"SELECT i, i % 1000, i::bigint * 31 FROM generate_series(1, 10000000) i"
	],
);

for my $scale (@scales)
{
	my ($label, $row_count, $schema, $gen_sql) = @$scale;
	my $heap_table  = "heap_${label}";
	my $recno_table = "recno_${label}";

	printf "\n--- %s: %s rows ---\n", $label, $row_count;

	# Create tables
	$node->safe_psql('postgres',
		"DROP TABLE IF EXISTS $heap_table CASCADE");
	$node->safe_psql('postgres',
		"DROP TABLE IF EXISTS $recno_table CASCADE");
	$node->safe_psql('postgres',
		"CREATE TABLE $heap_table $schema USING heap");
	$node->safe_psql('postgres',
		"CREATE TABLE $recno_table $schema USING recno");

	# Checkpoint before each test to start clean
	$node->safe_psql('postgres', 'CHECKPOINT');

	# HEAP insert
	my $heap_time =
	  timed_insert($node,
		"INSERT INTO $heap_table $gen_sql");
	my $heap_size       = relation_size($node, $heap_table);
	my $heap_size_human = relation_size_pretty($node, $heap_table);
	my $heap_tps        = sprintf("%.0f", $row_count / $heap_time);

	emit('heap', $row_count, 'insert_time_sec',
		sprintf("%.3f", $heap_time), 's');
	emit('heap', $row_count, 'throughput',    $heap_tps,        'rows/s');
	emit('heap', $row_count, 'table_size',    $heap_size,       'bytes');
	emit('heap', $row_count, 'table_size_hr', $heap_size_human, '');

	# Checkpoint between
	$node->safe_psql('postgres', 'CHECKPOINT');

	# RECNO insert
	my $recno_time =
	  timed_insert($node,
		"INSERT INTO $recno_table $gen_sql");
	my $recno_size       = relation_size($node, $recno_table);
	my $recno_size_human = relation_size_pretty($node, $recno_table);
	my $recno_tps        = sprintf("%.0f", $row_count / $recno_time);

	emit('recno', $row_count, 'insert_time_sec',
		sprintf("%.3f", $recno_time), 's');
	emit('recno', $row_count, 'throughput',    $recno_tps,        'rows/s');
	emit('recno', $row_count, 'table_size',    $recno_size,       'bytes');
	emit('recno', $row_count, 'table_size_hr', $recno_size_human, '');

	# Size comparison
	if ($heap_size > 0)
	{
		my $savings =
		  sprintf("%.1f", 100.0 * (1.0 - $recno_size / $heap_size));
		emit('comparison', $row_count, 'recno_savings_pct', $savings, '%');
		my $ratio = sprintf("%.2f", $heap_size / ($recno_size || 1));
		emit('comparison', $row_count, 'heap_to_recno_ratio', $ratio, 'x');
	}

	# Verify row counts match
	my $heap_count = $node->safe_psql('postgres',
		"SELECT count(*) FROM $heap_table");
	my $recno_count = $node->safe_psql('postgres',
		"SELECT count(*) FROM $recno_table");
	printf "  Verify: heap=%s recno=%s rows\n", $heap_count, $recno_count;

	# Clean up to free space for next test
	$node->safe_psql('postgres', "DROP TABLE $heap_table CASCADE");
	$node->safe_psql('postgres', "DROP TABLE $recno_table CASCADE");
}

close($csv);
$node->stop;

print "\n", "=" x 60, "\n";
print "Results written to: $csv_file\n";
print "=" x 60, "\n";
