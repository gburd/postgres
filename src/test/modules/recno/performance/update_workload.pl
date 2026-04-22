# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# Performance benchmark: Update workload for RECNO vs HEAP.
#
# Measures:
#   - In-place update effectiveness (RECNO advantage)
#   - 50/50 read/write mixed workload TPS via pgbench
#   - Storage bloat over successive update rounds
#
# Output: CSV file at performance/results/update_workload.csv

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

my $csv_file = "$results_dir/update_workload.csv";

my $node = PostgreSQL::Test::Cluster->new('bench_update');
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
CONF

$node->start;

open(my $csv, '>', $csv_file) or die "Cannot open $csv_file: $!";
print $csv "benchmark,access_method,phase,metric,value,unit\n";

sub emit
{
	my ($am, $phase, $metric, $value, $unit) = @_;
	print $csv "update_workload,$am,$phase,$metric,$value,$unit\n";
	printf "  %-6s %-30s %-24s %12s %s\n", $am, $phase, $metric, $value,
	  $unit;
}

sub relation_size
{
	my ($node, $table) = @_;
	return $node->safe_psql('postgres',
		"SELECT pg_relation_size('$table')");
}

sub dead_tuples
{
	my ($node, $table) = @_;
	# Force stats update
	$node->safe_psql('postgres', "ANALYZE $table");
	return $node->safe_psql('postgres',
		"SELECT n_dead_tup FROM pg_stat_user_tables WHERE relname = '$table'"
	);
}

print "=" x 60, "\n";
print "Update Workload Benchmark: RECNO vs HEAP\n";
print "=" x 60, "\n";

my $row_count = 100_000;

# ======================================================================
# Setup: Create identical tables
# ======================================================================
print "\n--- Setup: Loading $row_count rows into each table ---\n";

for my $am (qw(heap recno))
{
	$node->safe_psql('postgres',
		"DROP TABLE IF EXISTS ${am}_update CASCADE");
	$node->safe_psql('postgres', qq{
        CREATE TABLE ${am}_update (
            id       INT4 PRIMARY KEY,
            counter  INT4 NOT NULL DEFAULT 0,
            status   TEXT NOT NULL DEFAULT 'active',
            amount   NUMERIC(12,2),
            notes    TEXT
        ) USING $am
    });
	$node->safe_psql('postgres', qq{
        INSERT INTO ${am}_update
        SELECT i, 0, 'active',
               (random() * 10000)::numeric(12,2),
               'Initial note for record ' || i
        FROM generate_series(1, $row_count) i
    });
}

$node->safe_psql('postgres', 'CHECKPOINT');

# Baseline sizes
for my $am (qw(heap recno))
{
	my $size = relation_size($node, "${am}_update");
	emit($am, 'baseline', 'table_size', $size, 'bytes');
}

# ======================================================================
# Test 1: In-place counter increment (same-size update, RECNO sweet spot)
# ======================================================================
print "\n--- Test 1: In-place counter increment (50K rows) ---\n";

for my $am (qw(heap recno))
{
	my $t0 = [gettimeofday];
	$node->safe_psql('postgres',
		"UPDATE ${am}_update SET counter = counter + 1 WHERE id <= 50000");
	my $elapsed = tv_interval($t0);

	emit($am, 'inplace_50k', 'update_time_sec',
		sprintf("%.3f", $elapsed), 's');
	emit($am, 'inplace_50k', 'tps',
		sprintf("%.0f", 50000 / $elapsed), 'txn/s');
	emit($am, 'inplace_50k', 'table_size',
		relation_size($node, "${am}_update"), 'bytes');
}

# ======================================================================
# Test 2: Repeated update rounds (bloat accumulation)
# ======================================================================
print "\n--- Test 2: Repeated updates (10 rounds x 20K rows) ---\n";

my $rounds    = 10;
my $batch     = 20_000;

for my $am (qw(heap recno))
{
	my $total_t0 = [gettimeofday];
	for my $round (1 .. $rounds)
	{
		$node->safe_psql('postgres', qq{
            UPDATE ${am}_update
            SET counter = counter + 1,
                amount  = amount + 1.00
            WHERE id BETWEEN 1 AND $batch
        });

		# Record size after every 5th round
		if ($round % 5 == 0)
		{
			my $size = relation_size($node, "${am}_update");
			emit($am, "round_$round", 'table_size', $size, 'bytes');
		}
	}
	my $total_elapsed = tv_interval($total_t0);
	my $total_updates = $rounds * $batch;

	emit($am, 'repeated_total', 'update_time_sec',
		sprintf("%.3f", $total_elapsed), 's');
	emit($am, 'repeated_total', 'tps',
		sprintf("%.0f", $total_updates / $total_elapsed), 'txn/s');
	emit($am, 'repeated_total', 'table_size',
		relation_size($node, "${am}_update"), 'bytes');
}

# Bloat comparison (pre-VACUUM)
my $heap_size_pre  = relation_size($node, 'heap_update');
my $recno_size_pre = relation_size($node, 'recno_update');

if ($heap_size_pre > 0)
{
	my $savings = sprintf("%.1f",
		100.0 * (1.0 - $recno_size_pre / $heap_size_pre));
	emit('comparison', 'pre_vacuum', 'recno_savings_pct', $savings, '%');
}

# ======================================================================
# Test 3: Variable-length update (text field grows)
# ======================================================================
print "\n--- Test 3: Variable-length field update (30K rows) ---\n";

for my $am (qw(heap recno))
{
	my $t0 = [gettimeofday];
	$node->safe_psql('postgres', qq{
        UPDATE ${am}_update
        SET status = 'pending_review',
            notes  = 'Updated status at ' || now()::text ||
                     ' with additional context data appended'
        WHERE id BETWEEN 30001 AND 60000
    });
	my $elapsed = tv_interval($t0);

	emit($am, 'varlen_30k', 'update_time_sec',
		sprintf("%.3f", $elapsed), 's');
	emit($am, 'varlen_30k', 'table_size',
		relation_size($node, "${am}_update"), 'bytes');
}

# ======================================================================
# Test 4: Post-VACUUM size recovery
# ======================================================================
print "\n--- Test 4: VACUUM impact ---\n";

for my $am (qw(heap recno))
{
	my $pre = relation_size($node, "${am}_update");
	my $t0  = [gettimeofday];
	$node->safe_psql('postgres', "VACUUM ${am}_update");
	my $elapsed = tv_interval($t0);
	my $post    = relation_size($node, "${am}_update");

	emit($am, 'vacuum', 'vacuum_time_sec',
		sprintf("%.3f", $elapsed), 's');
	emit($am, 'vacuum', 'size_before', $pre,  'bytes');
	emit($am, 'vacuum', 'size_after',  $post, 'bytes');
	if ($pre > 0)
	{
		my $reclaimed =
		  sprintf("%.1f", 100.0 * (1.0 - $post / $pre));
		emit($am, 'vacuum', 'space_reclaimed_pct', $reclaimed, '%');
	}
}

# ======================================================================
# Test 5: pgbench mixed read/write workload (50/50)
# ======================================================================
print "\n--- Test 5: pgbench mixed workload (50/50 read/write, 30s) ---\n";

# Create pgbench workload script files
my $pgbench_dir = "$results_dir/pgbench_scripts";
make_path($pgbench_dir) unless -d $pgbench_dir;

for my $am (qw(heap recno))
{
	# Setup a clean workload table
	$node->safe_psql('postgres',
		"DROP TABLE IF EXISTS ${am}_pgbench CASCADE");
	$node->safe_psql('postgres', qq{
        CREATE TABLE ${am}_pgbench (
            id       INT4 PRIMARY KEY,
            counter  INT4 NOT NULL DEFAULT 0,
            balance  INT4 NOT NULL DEFAULT 0,
            filler   TEXT
        ) USING $am
    });
	$node->safe_psql('postgres', qq{
        INSERT INTO ${am}_pgbench
        SELECT i, 0, 0, repeat('x', 80)
        FROM generate_series(1, $row_count) i
    });
	$node->safe_psql('postgres', "ANALYZE ${am}_pgbench");

	# Write pgbench script: 50% update, 50% select
	my $script_path = "$pgbench_dir/${am}_mixed.sql";
	open(my $fh, '>', $script_path) or die "Cannot write $script_path: $!";
	print $fh <<EOF;
\\set aid random(1, $row_count)
\\set delta random(-100, 100)
BEGIN;
UPDATE ${am}_pgbench SET counter = counter + 1, balance = balance + :delta WHERE id = :aid;
SELECT id, counter, balance FROM ${am}_pgbench WHERE id = :aid;
COMMIT;
EOF
	close($fh);

	# Run pgbench
	my $connstr = $node->connstr('postgres');
	my $pgbench_out =
	  "$results_dir/pgbench_${am}_mixed.txt";
	my $t0 = [gettimeofday];

	# Use safe_psql to avoid requiring pgbench in PATH by running via psql
	# Instead, use the pgbench binary from the install
	my ($pgbench_stdout, $pgbench_stderr);
	my $pgbench_cmd = [
		'pgbench',
		'-c', '4',
		'-j', '2',
		'-T', '30',
		'-f', $script_path,
		'-d', 'postgres',
		'-h', $node->host,
		'-p', $node->port,
	];

	eval {
		IPC::Run::run($pgbench_cmd, '>', \$pgbench_stdout, '2>',
			\$pgbench_stderr)
		  or warn "pgbench exited with status $?";
	};
	my $elapsed = tv_interval($t0);

	if ($pgbench_stdout)
	{
		# Parse TPS from pgbench output
		if ($pgbench_stdout =~ /tps\s*=\s*([\d.]+)\s*\(excluding/m)
		{
			emit($am, 'pgbench_mixed', 'tps', sprintf("%.1f", $1), 'txn/s');
		}
		elsif ($pgbench_stdout =~ /tps\s*=\s*([\d.]+)/m)
		{
			emit($am, 'pgbench_mixed', 'tps', sprintf("%.1f", $1), 'txn/s');
		}

		# Save full output
		open(my $out, '>', $pgbench_out) or warn "Cannot write $pgbench_out";
		if ($out)
		{
			print $out $pgbench_stdout;
			print $out "\n--- stderr ---\n$pgbench_stderr"
			  if $pgbench_stderr;
			close($out);
		}
	}
	else
	{
		printf "  %-6s pgbench not available or failed: %s\n", $am,
		  ($pgbench_stderr // 'unknown error');
	}

	emit($am, 'pgbench_mixed', 'elapsed_sec',
		sprintf("%.1f", $elapsed), 's');
	emit($am, 'pgbench_mixed', 'table_size_after',
		relation_size($node, "${am}_pgbench"), 'bytes');
}

# ======================================================================
# Verify data integrity
# ======================================================================
print "\n--- Data Integrity Verification ---\n";
for my $am (qw(heap recno))
{
	my $count = $node->safe_psql('postgres',
		"SELECT count(*) FROM ${am}_update");
	printf "  %s_update: %s rows\n", $am, $count;
}

close($csv);
$node->stop;

print "\n", "=" x 60, "\n";
print "Results written to: $csv_file\n";
print "=" x 60, "\n";
