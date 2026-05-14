# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# Performance benchmark: Sequential scan performance for RECNO vs HEAP.
#
# Measures:
#   - Full table scan time (COUNT, SUM, AVG)
#   - Filtered scan with varying selectivity
#   - Text column scan (decompression overhead)
#   - I/O throughput (MB/s based on relation size / scan time)
#
# Output: CSV file at performance/results/sequential_scan.csv

use strict;
use warnings FATAL => 'all';

use File::Basename;
use File::Path qw(make_path);
use Time::HiRes qw(gettimeofday tv_interval);
use PostgreSQL::Test::Utils;
use PostgreSQL::Test::Cluster;

my $script_dir  = dirname(__FILE__);
my $results_dir = "$script_dir/results";
make_path($results_dir) unless -d $results_dir;

my $csv_file = "$results_dir/sequential_scan.csv";

my $node = PostgreSQL::Test::Cluster->new('bench_seqscan');
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
CONF

$node->start;

open(my $csv, '>', $csv_file) or die "Cannot open $csv_file: $!";
print $csv "benchmark,access_method,test,metric,value,unit\n";

sub emit
{
	my ($am, $test, $metric, $value, $unit) = @_;
	print $csv "sequential_scan,$am,$test,$metric,$value,$unit\n";
	printf "  %-6s %-28s %-22s %12s %s\n", $am, $test, $metric, $value,
	  $unit;
}

sub relation_size
{
	my ($node, $table) = @_;
	return $node->safe_psql('postgres',
		"SELECT pg_relation_size('$table')");
}

# Run a query N times, return average elapsed time
sub bench_query
{
	my ($node, $sql, $iterations) = @_;
	$iterations //= 3;
	my $total = 0;
	for my $i (1 .. $iterations)
	{
		# Drop OS caches between runs is not possible here,
		# but we can restart to clear shared buffers on first run.
		my $t0 = [gettimeofday];
		$node->safe_psql('postgres', $sql);
		$total += tv_interval($t0);
	}
	return $total / $iterations;
}

print "=" x 60, "\n";
print "Sequential Scan Benchmark: RECNO vs HEAP\n";
print "=" x 60, "\n";

my $row_count = 500_000;

# Force sequential scans only
$node->safe_psql('postgres', 'SET enable_indexscan = off');
$node->safe_psql('postgres', 'SET enable_bitmapscan = off');

# ======================================================================
# Setup: Create identical tables with mixed data types
# ======================================================================
print "\n--- Setup: Loading $row_count rows ---\n";

for my $am (qw(heap recno))
{
	$node->safe_psql('postgres',
		"DROP TABLE IF EXISTS ${am}_scan CASCADE");
	$node->safe_psql('postgres', qq{
        CREATE TABLE ${am}_scan (
            id       INT4,
            category INT4,
            amount   NUMERIC(12,2),
            label    TEXT,
            payload  TEXT
        ) USING $am
    });
	$node->safe_psql('postgres', qq{
        INSERT INTO ${am}_scan
        SELECT i,
               i % 100,
               (random() * 10000)::numeric(12,2),
               'Category-' || (i % 100) || '-Item-' || (i % 1000),
               'Detailed payload data for record ' || i ||
               '. This text is moderately long to test scan throughput ' ||
               'with compressed vs uncompressed storage.'
        FROM generate_series(1, $row_count) i
    });
	$node->safe_psql('postgres', "ANALYZE ${am}_scan");

	my $size = relation_size($node, "${am}_scan");
	emit($am, 'setup', 'table_size', $size, 'bytes');
	printf "  %s_scan: %s bytes (%s)\n", $am, $size,
	  $node->safe_psql('postgres',
		"SELECT pg_size_pretty(pg_relation_size('${am}_scan'))");
}

$node->safe_psql('postgres', 'CHECKPOINT');

# ======================================================================
# Test 1: Full table COUNT(*) -- minimal per-row processing
# ======================================================================
print "\n--- Test 1: Full Table COUNT(*) ---\n";

for my $am (qw(heap recno))
{
	my $avg_time = bench_query($node,
		"SET enable_indexscan = off; SET enable_bitmapscan = off; SELECT count(*) FROM ${am}_scan",
		5);
	my $size     = relation_size($node, "${am}_scan");
	my $throughput_mb =
	  $size > 0
	  ? sprintf("%.1f", ($size / 1048576.0) / $avg_time)
	  : '0';

	emit($am, 'count_star', 'avg_time_sec', sprintf("%.4f", $avg_time),
		's');
	emit($am, 'count_star', 'io_throughput', $throughput_mb, 'MB/s');
}

# ======================================================================
# Test 2: Aggregation (SUM, AVG, MIN, MAX)
# ======================================================================
print "\n--- Test 2: Aggregation ---\n";

for my $am (qw(heap recno))
{
	my $avg_time = bench_query($node, qq{
        SET enable_indexscan = off; SET enable_bitmapscan = off;
        SELECT count(*), avg(amount), sum(amount), min(amount), max(amount)
        FROM ${am}_scan
    }, 3);

	emit($am, 'aggregation', 'avg_time_sec', sprintf("%.4f", $avg_time),
		's');
}

# ======================================================================
# Test 3: Filtered scan (varying selectivity)
# ======================================================================
print "\n--- Test 3: Filtered Scans ---\n";

my @selectivities = (
	['1pct',  'category = 0',    0.01],
	['10pct', 'category < 10',   0.10],
	['50pct', 'category < 50',   0.50],
	['90pct', 'category < 90',   0.90],
);

for my $sel (@selectivities)
{
	my ($label, $filter, $fraction) = @$sel;

	for my $am (qw(heap recno))
	{
		my $avg_time = bench_query($node, qq{
            SET enable_indexscan = off; SET enable_bitmapscan = off;
            SELECT count(*), avg(amount) FROM ${am}_scan WHERE $filter
        }, 3);

		emit($am, "filter_$label", 'avg_time_sec',
			sprintf("%.4f", $avg_time), 's');
	}
}

# ======================================================================
# Test 4: Text column scan (forces decompression in RECNO)
# ======================================================================
print "\n--- Test 4: Text Column Scan ---\n";

for my $am (qw(heap recno))
{
	my $avg_time = bench_query($node, qq{
        SET enable_indexscan = off; SET enable_bitmapscan = off;
        SELECT count(*), avg(length(payload)), avg(length(label))
        FROM ${am}_scan
    }, 3);

	emit($am, 'text_scan', 'avg_time_sec', sprintf("%.4f", $avg_time),
		's');
}

# ======================================================================
# Test 5: GROUP BY aggregation (hash aggregate over full scan)
# ======================================================================
print "\n--- Test 5: GROUP BY Aggregation ---\n";

for my $am (qw(heap recno))
{
	my $avg_time = bench_query($node, qq{
        SET enable_indexscan = off; SET enable_bitmapscan = off;
        SELECT category, count(*), avg(amount), sum(amount)
        FROM ${am}_scan
        GROUP BY category
    }, 3);

	emit($am, 'group_by', 'avg_time_sec', sprintf("%.4f", $avg_time),
		's');
}

# ======================================================================
# Test 6: EXPLAIN ANALYZE for detailed metrics
# ======================================================================
print "\n--- Test 6: EXPLAIN ANALYZE ---\n";

for my $am (qw(heap recno))
{
	my $explain = $node->safe_psql('postgres', qq{
        SET enable_indexscan = off;
        SET enable_bitmapscan = off;
        EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
        SELECT count(*), sum(amount) FROM ${am}_scan
    });

	# Extract key metrics from EXPLAIN output
	if ($explain =~ /actual time=([\d.]+)\.\.([\d.]+)/m)
	{
		emit($am, 'explain', 'startup_time_ms', $1, 'ms');
		emit($am, 'explain', 'total_time_ms',   $2, 'ms');
	}
	if ($explain =~ /Buffers:\s*shared\s+hit=(\d+)/m)
	{
		emit($am, 'explain', 'shared_hit', $1, 'buffers');
	}
	if ($explain =~ /Buffers:\s*shared\s+hit=\d+\s+read=(\d+)/m)
	{
		emit($am, 'explain', 'shared_read', $1, 'buffers');
	}

	# Save full explain output
	my $explain_file = "$results_dir/explain_seqscan_${am}.txt";
	open(my $fh, '>', $explain_file) or warn "Cannot write $explain_file";
	if ($fh)
	{
		print $fh $explain;
		close($fh);
	}
}

close($csv);
$node->stop;

print "\n", "=" x 60, "\n";
print "Results written to: $csv_file\n";
print "=" x 60, "\n";
