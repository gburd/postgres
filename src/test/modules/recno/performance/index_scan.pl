# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# Performance benchmark: Index scan performance for RECNO vs HEAP.
#
# Measures:
#   - Point query latency via B-tree index
#   - Range scan performance (varying selectivity)
#   - Index-only scan vs index+heap-fetch comparison
#   - Query latency distribution (min, avg, p95, max)
#
# Output: CSV file at performance/results/index_scan.csv

use strict;
use warnings FATAL => 'all';

use File::Basename;
use File::Path qw(make_path);
use Time::HiRes qw(gettimeofday tv_interval);
use POSIX qw(floor);
use PostgreSQL::Test::Utils;
use PostgreSQL::Test::Cluster;

my $script_dir  = dirname(__FILE__);
my $results_dir = "$script_dir/results";
make_path($results_dir) unless -d $results_dir;

my $csv_file = "$results_dir/index_scan.csv";

my $node = PostgreSQL::Test::Cluster->new('bench_idxscan');
$node->init;

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
effective_cache_size = '512MB'
random_page_cost = 1.1
CONF

$node->start;

open(my $csv, '>', $csv_file) or die "Cannot open $csv_file: $!";
print $csv "benchmark,access_method,test,metric,value,unit\n";

sub emit
{
	my ($am, $test, $metric, $value, $unit) = @_;
	print $csv "index_scan,$am,$test,$metric,$value,$unit\n";
	printf "  %-6s %-28s %-22s %12s %s\n", $am, $test, $metric, $value,
	  $unit;
}

sub relation_size
{
	my ($node, $table) = @_;
	return $node->safe_psql('postgres',
		"SELECT pg_relation_size('$table')");
}

# Run a query many times and collect latency samples
sub bench_latency
{
	my ($node, $sql_template, $iterations, $id_max) = @_;
	$iterations //= 1000;
	$id_max     //= 500_000;

	my @latencies;
	for my $i (1 .. $iterations)
	{
		my $id  = int(rand($id_max)) + 1;
		my $sql = $sql_template;
		$sql =~ s/\$ID/$id/g;

		my $t0 = [gettimeofday];
		$node->safe_psql('postgres', $sql);
		push @latencies, tv_interval($t0);
	}

	@latencies = sort { $a <=> $b } @latencies;
	my $n   = scalar @latencies;
	my $sum = 0;
	$sum += $_ for @latencies;

	return {
		min  => $latencies[0],
		avg  => $sum / $n,
		p50  => $latencies[floor($n * 0.50)],
		p95  => $latencies[floor($n * 0.95)],
		p99  => $latencies[floor($n * 0.99)],
		max  => $latencies[$n - 1],
		n    => $n,
	};
}

print "=" x 60, "\n";
print "Index Scan Benchmark: RECNO vs HEAP\n";
print "=" x 60, "\n";

my $row_count = 500_000;

# ======================================================================
# Setup: Create indexed tables
# ======================================================================
print "\n--- Setup: Loading $row_count rows with indexes ---\n";

for my $am (qw(heap recno))
{
	$node->safe_psql('postgres',
		"DROP TABLE IF EXISTS ${am}_idx CASCADE");
	$node->safe_psql('postgres', qq{
        CREATE TABLE ${am}_idx (
            id       INT4 PRIMARY KEY,
            category INT4 NOT NULL,
            amount   NUMERIC(12,2),
            status   TEXT NOT NULL,
            payload  TEXT
        ) USING $am
    });
	$node->safe_psql('postgres', qq{
        INSERT INTO ${am}_idx
        SELECT i,
               i % 100,
               (random() * 10000)::numeric(12,2),
               CASE WHEN i % 5 = 0 THEN 'inactive'
                    WHEN i % 3 = 0 THEN 'pending'
                    ELSE 'active' END,
               'Payload data for record ' || i
        FROM generate_series(1, $row_count) i
    });

	# Create secondary indexes
	$node->safe_psql('postgres',
		"CREATE INDEX ${am}_idx_category ON ${am}_idx(category)");
	$node->safe_psql('postgres',
		"CREATE INDEX ${am}_idx_amount ON ${am}_idx(amount)");
	$node->safe_psql('postgres',
		"CREATE INDEX ${am}_idx_status ON ${am}_idx(status)");

	$node->safe_psql('postgres', "ANALYZE ${am}_idx");

	my $tbl_size = relation_size($node, "${am}_idx");
	emit($am, 'setup', 'table_size', $tbl_size, 'bytes');
}

$node->safe_psql('postgres', 'CHECKPOINT');

# ======================================================================
# Test 1: Primary key point queries
# ======================================================================
print "\n--- Test 1: PK Point Query (1000 random lookups) ---\n";

for my $am (qw(heap recno))
{
	my $stats = bench_latency($node,
		"SELECT * FROM ${am}_idx WHERE id = \$ID", 1000, $row_count);

	emit($am, 'pk_point', 'avg_ms',
		sprintf("%.3f", $stats->{avg} * 1000), 'ms');
	emit($am, 'pk_point', 'p50_ms',
		sprintf("%.3f", $stats->{p50} * 1000), 'ms');
	emit($am, 'pk_point', 'p95_ms',
		sprintf("%.3f", $stats->{p95} * 1000), 'ms');
	emit($am, 'pk_point', 'p99_ms',
		sprintf("%.3f", $stats->{p99} * 1000), 'ms');
	emit($am, 'pk_point', 'min_ms',
		sprintf("%.3f", $stats->{min} * 1000), 'ms');
	emit($am, 'pk_point', 'max_ms',
		sprintf("%.3f", $stats->{max} * 1000), 'ms');
}

# ======================================================================
# Test 2: Secondary index point queries
# ======================================================================
print "\n--- Test 2: Secondary Index Point Query (category, 500 lookups) ---\n";

for my $am (qw(heap recno))
{
	my $stats = bench_latency($node,
		"SELECT count(*) FROM ${am}_idx WHERE category = (\$ID % 100)",
		500, $row_count);

	emit($am, 'sec_point', 'avg_ms',
		sprintf("%.3f", $stats->{avg} * 1000), 'ms');
	emit($am, 'sec_point', 'p95_ms',
		sprintf("%.3f", $stats->{p95} * 1000), 'ms');
}

# ======================================================================
# Test 3: Range scans (varying selectivity)
# ======================================================================
print "\n--- Test 3: Range Scans ---\n";

my @ranges = (
	['range_10',    10],
	['range_100',   100],
	['range_1000',  1000],
	['range_10000', 10000],
);

for my $range (@ranges)
{
	my ($label, $width) = @$range;

	for my $am (qw(heap recno))
	{
		my $stats = bench_latency($node,
			"SELECT count(*), sum(amount) FROM ${am}_idx WHERE id BETWEEN \$ID AND \$ID + $width",
			200, $row_count - $width);

		emit($am, $label, 'avg_ms',
			sprintf("%.3f", $stats->{avg} * 1000), 'ms');
		emit($am, $label, 'p95_ms',
			sprintf("%.3f", $stats->{p95} * 1000), 'ms');
	}
}

# ======================================================================
# Test 4: Index-only scan (covering index)
# ======================================================================
print "\n--- Test 4: Index-Only Scan ---\n";

for my $am (qw(heap recno))
{
	# VACUUM to set visibility map (required for index-only scans)
	$node->safe_psql('postgres', "VACUUM ${am}_idx");

	# id is PK, so "SELECT id" should use index-only scan
	my $stats = bench_latency($node,
		"SELECT id FROM ${am}_idx WHERE id = \$ID", 1000, $row_count);

	emit($am, 'idx_only', 'avg_ms',
		sprintf("%.3f", $stats->{avg} * 1000), 'ms');
	emit($am, 'idx_only', 'p95_ms',
		sprintf("%.3f", $stats->{p95} * 1000), 'ms');

	# Verify it actually uses index-only scan
	my $plan = $node->safe_psql('postgres',
		"EXPLAIN (COSTS OFF) SELECT id FROM ${am}_idx WHERE id = 42");
	if ($plan =~ /Index Only Scan/i)
	{
		emit($am, 'idx_only', 'uses_index_only_scan', 1, 'bool');
	}
	else
	{
		emit($am, 'idx_only', 'uses_index_only_scan', 0, 'bool');
	}
}

# ======================================================================
# Test 5: Batch point queries (prepared statement simulation)
# ======================================================================
print "\n--- Test 5: Batch Point Queries (100 per batch, 50 batches) ---\n";

for my $am (qw(heap recno))
{
	my @batch_times;
	for my $batch (1 .. 50)
	{
		# Generate 100 random IDs
		my @ids = map { int(rand($row_count)) + 1 } (1 .. 100);
		my $id_list = join(',', @ids);

		my $t0 = [gettimeofday];
		$node->safe_psql('postgres',
			"SELECT * FROM ${am}_idx WHERE id IN ($id_list)");
		push @batch_times, tv_interval($t0);
	}

	@batch_times = sort { $a <=> $b } @batch_times;
	my $n   = scalar @batch_times;
	my $sum = 0;
	$sum += $_ for @batch_times;

	emit($am, 'batch_100', 'avg_ms',
		sprintf("%.3f", ($sum / $n) * 1000), 'ms');
	emit($am, 'batch_100', 'p95_ms',
		sprintf("%.3f", $batch_times[floor($n * 0.95)] * 1000), 'ms');
}

# ======================================================================
# Test 6: EXPLAIN ANALYZE for index scan details
# ======================================================================
print "\n--- Test 6: EXPLAIN ANALYZE ---\n";

for my $am (qw(heap recno))
{
	# Point query EXPLAIN
	my $explain_point = $node->safe_psql('postgres', qq{
        EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
        SELECT * FROM ${am}_idx WHERE id = 42
    });

	if ($explain_point =~ /actual time=([\d.]+)\.\.([\d.]+)/m)
	{
		emit($am, 'explain_point', 'startup_ms', $1, 'ms');
		emit($am, 'explain_point', 'total_ms',   $2, 'ms');
	}
	if ($explain_point =~ /Buffers:\s*shared\s+hit=(\d+)/m)
	{
		emit($am, 'explain_point', 'shared_hit', $1, 'buffers');
	}

	# Range query EXPLAIN
	my $explain_range = $node->safe_psql('postgres', qq{
        EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
        SELECT count(*), sum(amount) FROM ${am}_idx
        WHERE id BETWEEN 1000 AND 2000
    });

	if ($explain_range =~ /actual time=([\d.]+)\.\.([\d.]+)/m)
	{
		emit($am, 'explain_range', 'startup_ms', $1, 'ms');
		emit($am, 'explain_range', 'total_ms',   $2, 'ms');
	}
	if ($explain_range =~ /Buffers:\s*shared\s+hit=(\d+)/m)
	{
		emit($am, 'explain_range', 'shared_hit', $1, 'buffers');
	}

	# Save full EXPLAIN output
	for my $pair (
		['point', $explain_point],
		['range', $explain_range])
	{
		my ($type, $text) = @$pair;
		my $explain_file = "$results_dir/explain_idxscan_${am}_${type}.txt";
		open(my $fh, '>', $explain_file) or warn "Cannot write $explain_file";
		if ($fh)
		{
			print $fh $text;
			close($fh);
		}
	}
}

close($csv);
$node->stop;

print "\n", "=" x 60, "\n";
print "Results written to: $csv_file\n";
print "=" x 60, "\n";
